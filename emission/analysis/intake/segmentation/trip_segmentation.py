import logging

import emission.storage.timeseries.abstract_timeseries as esta
import emission.storage.decorations.place_queries as esdp
import emission.storage.decorations.analysis_timeseries_queries as esda
import emission.storage.pipeline_queries as epq

import emission.core.wrapper.transition as ecwt
import emission.core.wrapper.location as ecwl
import emission.core.wrapper.rawtrip as ecwrt
import emission.core.wrapper.rawplace as ecwrp
import emission.core.wrapper.entry as ecwe

class TripSegmentationMethod(object):
    def segment_into_trips(self, timeseries, time_query):
        """
        Examines the timeseries database for a specific range and returns the
        segmentation points. Note that the input is the entire timeseries and
        the time range. This allows algorithms to use whatever combination of
        data that they want from the sensor streams in order to determine the
        segmentation points.
        
        Returns array of location point tuples corresponding to the start and end of
        trips in this time range. The first trip returned starts with the first
        location point in the time range.
    
        i.e. [(start1, end1), (start2, end2)...]. Points between end1 and
        start2 are assumed to be within place 2 and are generally ignored.

        If there are no segments, returns an empty array.
        If the method is not yet ready to segment the trips (e.g. maybe it is
            waiting until the end of the day in order to use a clustering algorithm, returns None)
        """
        pass
    
def segment_current_trips(user_id):
    ts = esta.TimeSeries.get_time_series(user_id)
    time_query = epq.get_time_range_for_segmentation(user_id)

    import emission.analysis.intake.segmentation.trip_segmentation_methods.dwell_segmentation_time_filter as dstf
    import emission.analysis.intake.segmentation.trip_segmentation_methods.dwell_segmentation_dist_filter as dsdf
    dstfsm = dstf.DwellSegmentationTimeFilter(time_threshold = 5 * 60, # 5 mins
                                              point_threshold = 9,
                                              distance_threshold = 100) # 100 m

    dsdfsm = dsdf.DwellSegmentationDistFilter(time_threshold = 10 * 60, # 10 mins
                                              point_threshold = 9,
                                              distance_threshold = 50) # 50 m

    filter_methods = {"time": dstfsm, "distance": dsdfsm}
    filter_method_names = {"time": "DwellSegmentationTimeFilter", "distance": "DwellSegmentationDistFilter"}
    # We need to use the appropriate filter based on the incoming data
    # So let's read in the location points for the specified query
    loc_df = ts.get_data_df("background/filtered_location", time_query)
    if len(loc_df) == 0:
        # no new segments, no need to keep looking at these again
        logging.debug("len(loc_df) == 0, early return")
        epq.mark_segmentation_done(user_id, None)
        return

    filters_in_df = loc_df["filter"].unique()
    logging.debug("Filters in the dataframe = %s" % filters_in_df)
    if len(filters_in_df) == 1:
        # Common case - let's make it easy
        
        segmentation_points = filter_methods[filters_in_df[0]].segment_into_trips(ts,
            time_query)
    else:
        segmentation_points = get_combined_segmentation_points(ts, loc_df, time_query,
                                                               filters_in_df,
                                                               filter_methods)
    # Create and store trips and places based on the segmentation points
    if segmentation_points is None:
        epq.mark_segmentation_failed(user_id)
    elif len(segmentation_points) == 0:
        # no new segments, no need to keep looking at these again
        logging.debug("len(segmentation_points) == 0, early return")
        epq.mark_segmentation_done(user_id, None)
    else:
        try:
            create_places_and_trips(user_id, segmentation_points, filter_method_names[filters_in_df[0]])
            epq.mark_segmentation_done(user_id, get_last_ts_processed(filter_methods))
        except:
            logging.exception("Trip generation failed for user %s" % user_id)
            epq.mark_segmentation_failed(user_id)
            
def get_combined_segmentation_points(ts, loc_df, time_query, filters_in_df, filter_methods):
    """
    We can have mixed filters in a particular time range for multiple reasons.
    a) user switches phones from one platform to another
    b) user signs in simultaneously to phones on both platforms
    case (b) is not a supported case, and can happen even with the same platform - i.e. a user can sign in to two devices of the same platform
    (e.g. tablet and phone) with the same ID and then the trips won't really
    match up.
    So let's handle case (a), which should be a supported use case, 
    by creating separate time queries for the various types of filters and
    combining them based on the order in which the filters appear in the dataframe.
    Note that another option is to filter the dataframe inside the segmentation method
    but then you would have to figure out how to recombine the segmentation points
    maybe sort the resulting segmentation points by start_ts?
    That might be the easier option after all
    """
    segmentation_map = {}
    for curr_filter in filters_in_df:
        time_query.startTs = loc_df[loc_df["filter"] == curr_filter].head(1).iloc[0].ts
        time_query.endTs = loc_df[loc_df["filter"] == curr_filter].tail(1).iloc[0].ts
        logging.debug("for filter %s, startTs = %d and endTs = %d" %
            (curr_filter, time_query.startTs, time_query.endTs))
        segmentation_map[time_query.startTs] = filter_methods[curr_filter].segment_into_trips(ts, time_query)
    logging.debug("After filtering, segmentation_map has keys %s" % segmentation_map.keys())
    sortedStartTsList = sorted(segmentation_map.keys())
    segmentation_points = []
    for startTs in sortedStartTsList:
        segmentation_points.extend(segmentation_map[startTs])
    return segmentation_points

def get_last_ts_processed(filter_methods):
    last_ts_processed = None
    for method in filter_methods.itervalues():
        try:
            if last_ts_processed is None or method.last_ts_processed > last_ts_processed:
                last_ts_processed = method.last_ts_processed
                logging.debug("Set last_ts_processed = %s from method %s" % (last_ts_processed, method))
        except AttributeError, e:
            logging.debug("Processing method %s got error %s, skipping" % (method, e))
    logging.info("Returning last_ts_processed = %s" % last_ts_processed)
    return last_ts_processed

def create_places_and_trips(user_id, segmentation_points, segmentation_method_name):
    # new segments, need to deal with them
    # First, retrieve the last place so that we can stitch it to the newly created trip.
    # Again, there are easy and hard. In the easy case, the trip was
    # continuous, was stopped when the trip end was detected, and there is
    # no gap between the start of the trip and the last place. But there
    # can be other issues caused by gaps in tracking. A more detailed
    # description of dealing with gaps in tracking can be found in the wiki.
    # Let us first deal with the easy case.
    # restart_events_df = get_restart_events(ts, time_query)
    ts = esta.TimeSeries.get_time_series(user_id)
    last_place_entry = esdp.get_last_place_entry(esda.RAW_PLACE_KEY, user_id)
    if last_place_entry is None:
        last_place = start_new_chain(user_id)
        last_place.source = segmentation_method_name
        last_place_entry = ecwe.Entry.create_entry(user_id,
                                "segmentation/raw_place", last_place, create_id = True)
    else:
        last_place = last_place_entry.data

    # if is_easy_case(restart_events_df):
    # Theoretically, we can do some sanity checks here to make sure
    # that we are fairly close to the last point. Maybe mark some kind
    # of confidence level based on that?
    logging.debug("segmentation_point_list has length %s" % len(segmentation_points))
    for (start_loc_doc, end_loc_doc) in segmentation_points:
        logging.debug("start_loc_doc = %s, end_loc_doc = %s" % (start_loc_doc, end_loc_doc))
        get_loc_for_row = lambda row: ts.df_row_to_entry("background/filtered_location", row).data
        start_loc = get_loc_for_row(start_loc_doc)
        end_loc = get_loc_for_row(end_loc_doc)
        logging.debug("start_loc = %s, end_loc = %s" % (start_loc, end_loc))

        # Stitch together the last place and the current trip
        curr_trip = ecwrt.Rawtrip()
        curr_trip.source = segmentation_method_name
        curr_trip_entry = ecwe.Entry.create_entry(user_id,
                            "segmentation/raw_trip", curr_trip, create_id = True)

        new_place = ecwrp.Rawplace()
        new_place.source = segmentation_method_name
        new_place_entry = ecwe.Entry.create_entry(user_id,
                            "segmentation/raw_place", new_place, create_id = True)

        stitch_together_start(last_place_entry, curr_trip_entry, start_loc)
        stitch_together_end(new_place_entry, curr_trip_entry, end_loc)

        ts.insert(curr_trip_entry)
        # last_place is a copy of the data in this entry. So after we fix it
        # the way we want, we need to assign it back to the entry, otherwise
        # it will be lost
        ts.update(last_place_entry)
        last_place_entry = new_place_entry

    # The last last_place hasn't been stitched together yet, but we
    # need to save it so that it can be the last_place for the next run
    ts.insert(last_place_entry)

def start_new_chain(uuid):
    """
    Can't find the place that is the end of an existing chain, so we need to
    create a new one.  This might correspond to the start of tracking, or to an
    improperly terminated chain. For now, we deal with the start of tracking,
    and add the checks for the improperly terminated chain later.
    TODO: Add checks for improperly terminated chains later.
    """
    start_place = ecwrp.Rawplace()
    logging.debug("Starting tracking, created new start of chain %s" % start_place)
    return start_place

def stitch_together_start(last_place_entry, curr_trip_entry, start_loc):
    """
    Stitch together the last place and the current trip at the start location.
    Note that we don't actually know the time that we left the start place
    because we are only invoked when we have exited the geofence. We can do
    something fancy with extraploation based on average speed, but let's keep
    the fuzz factor for now.
    """
    last_place = last_place_entry.data
    curr_trip = curr_trip_entry.data

    last_place.exit_ts = start_loc.ts
    last_place.exit_local_dt = start_loc.local_dt
    last_place.exit_fmt_time = start_loc.fmt_time
    last_place.starting_trip = curr_trip_entry.get_id()
    if "enter_ts" in last_place:
        last_place.duration = last_place.exit_ts - last_place.enter_ts
    else:
        logging.debug("Place %s is the start of tracking - duration not known" % last_place)
        # Since this is the first place, it didn't have its location set at the end of a trip
        # in stitch_together_end. So we set it here. Note that this is likely to be off by
        # a bit because this is actually the start of the trip, but it is not too bad.
        last_place.location = start_loc.loc

    curr_trip.start_ts = start_loc.ts
    curr_trip.start_local_dt = start_loc.local_dt
    curr_trip.start_fmt_time = start_loc.fmt_time
    curr_trip.start_place = last_place_entry.get_id()
    curr_trip.start_loc = start_loc.loc

    # The wrapper class returns a copy of the data object, so any changes to it
    # are not reflected in the original
    last_place_entry["data"] = last_place
    curr_trip_entry["data"] = curr_trip


def stitch_together_end(new_place_entry, curr_trip_entry, end_loc):
    """
    Stitch together the last place and the current trip at the start location.
    Note that we don't actually know the time that we left the start place
    because we are only invoked when we have exited the geofence. We can do
    something fancy with extraploation based on average speed, but let's keep
    the fuzz factor for now.
    """
    new_place = new_place_entry.data
    curr_trip = curr_trip_entry.data

    curr_trip.end_ts = end_loc.ts
    curr_trip.end_local_dt = end_loc.local_dt
    curr_trip.end_fmt_time = end_loc.fmt_time
    curr_trip.end_place = new_place_entry.get_id()
    curr_trip.end_loc = end_loc.loc
    curr_trip.duration = curr_trip.end_ts - curr_trip.start_ts

    new_place.enter_ts = end_loc.ts
    new_place.enter_local_dt = end_loc.local_dt
    new_place.enter_fmt_time = end_loc.fmt_time
    new_place.ending_trip = curr_trip_entry.get_id()
    new_place.location = end_loc.loc

    # The wrapper class returns a copy of the data object, so any changes to it
    # are not reflected in the original
    new_place_entry["data"] = new_place
    curr_trip_entry["data"] = curr_trip


def get_restart_events(timeseries, time_query):
    transition_df = timeseries.get_data_df("statemachine/transition", time_query)
    restart_events_df = transition_df.query('transition' == ecwt.TransitionType.BOOTED.value or
                                            'transition' == ecwt.TransitionType.INITIALIZE.value or
                                            'transition' == ecwt.TransitionType.STOP_TRACKING.value)

def is_easy_case(restart_events_df):
    return len(restart_events_df) == 0
