import logging

import emission.storage.timeseries.abstract_timeseries as esta
import emission.storage.decorations.place_queries as esdp
import emission.storage.decorations.trip_queries as esdt
import emission.storage.pipeline_queries as epq

import emission.core.wrapper.transition as ecwt
import emission.core.wrapper.location as ecwl
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
    dstfsm = dstf.DwellSegmentationTimeFilter(time_threshold = 5 * 60, # 5 mins
                                              point_threshold = 9,
                                              distance_threshold = 100) # 100 m

    segmentation_points = dstfsm.segment_into_trips(ts, time_query)
    # Create and store trips and places based on the segmentation points
    if segmentation_points is None:
        epq.mark_segmentation_failed(user_id)
    elif len(segmentation_points) == 0:
        # no new segments, no need to keep looking at these again
        logging.debug("len(segmentation_points) == 0, early return")
        epq.mark_segmentation_done(user_id, None)
    else:
        try:
            create_places_and_trips(user_id, segmentation_points)
            epq.mark_segmentation_done(user_id, dstfsm.last_ts_processed)
        except:
            logging.exception("Trip generation failed for user %s" % user_id)
            epq.mark_segmentation_failed(user_id)

def create_places_and_trips(user_id, segmentation_points):
    # new segments, need to deal with them
    # First, retrieve the last place so that we can stitch it to the newly created trip.
    # Again, there are easy and hard. In the easy case, the trip was
    # continuous, was stopped when the trip end was detected, and there is
    # no gap between the start of the trip and the last place. But there
    # can be other issues caused by gaps in tracking. A more detailed
    # description of dealing with gaps in tracking can be found in the wiki.
    # Let us first deal with the easy case.
    # restart_events_df = get_restart_events(ts, time_query)
    last_place = esdp.get_last_place(user_id)
    if last_place is None:
        last_place = start_new_chain(user_id)

    # if is_easy_case(restart_events_df):
    # Theoretically, we can do some sanity checks here to make sure
    # that we are fairly close to the last point. Maybe mark some kind
    # of confidence level based on that?
    logging.debug("segmentation_point_list has length %s" % len(segmentation_points))
    for (start_loc_doc, end_loc_doc) in segmentation_points:
        logging.debug("start_loc_doc = %s, end_loc_doc = %s" % (start_loc_doc, end_loc_doc))
        start_loc = ecwl.Location(start_loc_doc)
        end_loc = ecwl.Location(end_loc_doc)
        logging.debug("start_loc = %s, end_loc = %s" % (start_loc, end_loc))

        # Stitch together the last place and the current trip
        curr_trip = esdt.create_new_trip(user_id)
        new_place = esdp.create_new_place(user_id)

        stitch_together_start(last_place, curr_trip, start_loc)
        stitch_together_end(new_place, curr_trip, end_loc)

        esdp.save_place(last_place)
        esdt.save_trip(curr_trip)

        last_place = new_place

    # The last last_place hasn't been stitched together yet, but we
    # need to save it so that it can be the last_place for the next run
    esdp.save_place(last_place)

def start_new_chain(uuid):
    """
    Can't find the place that is the end of an existing chain, so we need to
    create a new one.  This might correspond to the start of tracking, or to an
    improperly terminated chain. For now, we deal with the start of tracking,
    and add the checks for the improperly terminated chain later.
    TODO: Add checks for improperly terminated chains later.
    """
    start_place = esdp.create_new_place(uuid)
    logging.debug("Starting tracking, created new start of chain %s" % start_place)
    return start_place

def stitch_together_start(last_place, curr_trip, start_loc):
    """
    Stitch together the last place and the current trip at the start location.
    Note that we don't actually know the time that we left the start place
    because we are only invoked when we have exited the geofence. We can do
    something fancy with extraploation based on average speed, but let's keep
    the fuzz factor for now.
    """
    last_place.exit_ts = start_loc.ts
    last_place.exit_local_dt = start_loc.local_dt
    last_place.exit_fmt_time = start_loc.fmt_time
    last_place.starting_trip = curr_trip.get_id()
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
    curr_trip.start_place = last_place.get_id()
    curr_trip.start_loc = start_loc.loc
    curr_trip.source = "DwellSegmentationTimeFilter"

def stitch_together_end(new_place, curr_trip, end_loc):
    """
    Stitch together the last place and the current trip at the start location.
    Note that we don't actually know the time that we left the start place
    because we are only invoked when we have exited the geofence. We can do
    something fancy with extraploation based on average speed, but let's keep
    the fuzz factor for now.
    """
    curr_trip.end_ts = end_loc.ts
    curr_trip.end_local_dt = end_loc.local_dt
    curr_trip.end_fmt_time = end_loc.fmt_time
    curr_trip.end_place = new_place.get_id()
    curr_trip.end_loc = end_loc.loc
    curr_trip.duration = curr_trip.end_ts - curr_trip.start_ts

    new_place.enter_ts = end_loc.ts
    new_place.enter_local_dt = end_loc.local_dt
    new_place.enter_fmt_time = end_loc.fmt_time
    new_place.ending_trip = curr_trip.get_id()
    new_place.location = end_loc.loc
    new_place.source = "DwellSegmentationTimeFilter"

def get_restart_events(timeseries, time_query):
    transition_df = timeseries.get_data_df("statemachine/transition", time_query)
    restart_events_df = transition_df.query('transition' == ecwt.TransitionType.BOOTED.value or
                                            'transition' == ecwt.TransitionType.INITIALIZE.value or
                                            'transition' == ecwt.TransitionType.STOP_TRACKING.value)

def is_easy_case(restart_events_df):
    return len(restart_events_df) == 0
