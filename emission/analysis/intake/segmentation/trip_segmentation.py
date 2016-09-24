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
import emission.core.wrapper.untrackedtime as ecwut

import emission.core.common as ecc

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

    out_of_order_points = loc_df[loc_df.ts.diff() < 0]
    if len(out_of_order_points) > 0:
        logging.info("Found out of order points!")
        logging.info("%s" % out_of_order_points)
        # drop from the table
        loc_df = loc_df.drop(out_of_order_points.index.tolist())
        # delete from the database. Should be generally discouraged, so we
        # are kindof putting it in here secretively
        import emission.core.get_database as edb

        out_of_order_id_list = out_of_order_points["_id"].tolist()
        logging.debug("out_of_order_id_list = %s" % out_of_order_id_list)
        edb.get_timeseries_db().remove({"_id": {"$in": out_of_order_id_list}})

    filters_in_df = loc_df["filter"].dropna().unique()
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

        if found_untracked_period(ts, last_place_entry.data, start_loc):
            # Fill in the gap in the chain with an untracked period
            curr_untracked = ecwut.Untrackedtime()
            curr_untracked.source = segmentation_method_name
            curr_untracked_entry = ecwe.Entry.create_entry(user_id,
                            "segmentation/raw_untracked", curr_untracked, create_id=True)

            restarted_place = ecwrp.Rawplace()
            restarted_place.source = segmentation_method_name
            restarted_place_entry = ecwe.Entry.create_entry(user_id,
                            "segmentation/raw_place", restarted_place, create_id=True)

            untracked_start_loc = ecwe.Entry(ts.get_entry_at_ts("background/filtered_location",
                                                     "data.ts", last_place_entry.data.enter_ts)).data
            untracked_start_loc["ts"] = untracked_start_loc.ts + epq.END_FUZZ_AVOID_LTE
            _link_and_save(ts, last_place_entry, curr_untracked_entry, restarted_place_entry,
                           untracked_start_loc, start_loc)
            logging.debug("Created untracked period %s from %s to %s" %
                          (curr_untracked_entry.get_id(), curr_untracked_entry.data.start_ts, curr_untracked_entry.data.end_ts))
            logging.debug("Resetting last_place_entry from %s to %s" %
                          (last_place_entry, restarted_place_entry))
            last_place_entry = restarted_place_entry

        _link_and_save(ts, last_place_entry, curr_trip_entry, new_place_entry, start_loc, end_loc)
        last_place_entry = new_place_entry

    # The last last_place hasn't been stitched together yet, but we
    # need to save it so that it can be the last_place for the next run
    ts.insert(last_place_entry)

def _link_and_save(ts, last_place_entry, curr_trip_entry, new_place_entry, start_loc, end_loc):
    stitch_together_start(last_place_entry, curr_trip_entry, start_loc)
    stitch_together_end(new_place_entry, curr_trip_entry, end_loc)

    ts.insert(curr_trip_entry)
    # last_place is a copy of the data in this entry. So after we fix it
    # the way we want, we need to assign it back to the entry, otherwise
    # it will be lost
    ts.update(last_place_entry)

def found_untracked_period(timeseries, last_place, start_loc):
    """
    Check to see whether the two places are the same.
    This is a fix for https://github.com/e-mission/e-mission-server/issues/378
    Note both last_place and start_loc are data wrappers (e.g. RawPlace and Location objects)
    NOT entries. So field access should not be preceeded by "data"

    :return: True if we should create a new start place instead of linking to
    the last_place, False otherwise
    """
    # Implementing logic from https://github.com/e-mission/e-mission-server/issues/378
    if last_place.enter_ts is None:
        logging.debug("last_place.enter_ts = %s" % (last_place.enter_ts))
        logging.debug("start of a chain, unable to check for restart from previous trip end, assuming not restarted")
        return False

    if _is_tracking_restarted(last_place, start_loc, timeseries):
        logging.debug("tracking has been restarted, returning True")
        return True

    transition_distance = ecc.calDistance(last_place.location.coordinates,
                       start_loc.loc.coordinates)
    logging.debug("while determining new_start_place, transition_distance = %s" % transition_distance)
    if transition_distance < 1000:
        logging.debug("transition_distance %s < 1000, returning False", transition_distance)
        return False

    time_delta = start_loc.ts - last_place.enter_ts
    transition_speed = transition_distance / time_delta
    logging.debug("while determining new_start_place, time_delta = %s, transition_speed = %s"
                  % (time_delta, transition_speed))

    # Let's use a little less than walking speed 3km/hr < 3mph (4.83 kmph)
    speed_threshold = float(3000) / (60*60)

    if transition_speed > speed_threshold:
        logging.debug("transition_speed %s > %s, returning False" %
                      (transition_speed, speed_threshold))
        return False
    else:
        logging.debug("transition_speed %s <= %s, 'stopped', returning True" %
                        (transition_speed, speed_threshold))
        return True

def start_new_chain(uuid):
    """
    Can't find the place that is the end of an existing chain, so we need to
    create a new one.  This might correspond to the start of tracking, or to an
    improperly terminated chain.
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
    curr_trip.distance = ecc.calDistance(curr_trip.end_loc.coordinates,
                                         curr_trip.start_loc.coordinates)

    new_place.enter_ts = end_loc.ts
    new_place.enter_local_dt = end_loc.local_dt
    new_place.enter_fmt_time = end_loc.fmt_time
    new_place.ending_trip = curr_trip_entry.get_id()
    new_place.location = end_loc.loc

    # The wrapper class returns a copy of the data object, so any changes to it
    # are not reflected in the original
    new_place_entry["data"] = new_place
    curr_trip_entry["data"] = curr_trip

def _is_tracking_restarted(last_place, start_loc, timeseries):
    import emission.storage.timeseries.timequery as estt

    tq = estt.TimeQuery(timeType="data.ts", startTs=last_place.enter_ts,
                        endTs=start_loc.ts)
    transition_df = timeseries.get_data_df("statemachine/transition", tq)
    if len(transition_df) == 0:
        logging.debug("In range %s -> %s found no transitions" %
                      (tq.startTs, tq.endTs))
        return False
    logging.debug("In range %s -> %s found transitions %s" %
                  (tq.startTs, tq.endTs, transition_df[["fmt_time", "curr_state", "transition"]]))
    return _is_tracking_restarted_android(transition_df) or \
           _is_tracking_restarted_ios(transition_df)

def _is_tracking_restarted_android(transition_df):
    """
    Life is fairly simple on android. There are two main cases that cause tracking
    to be off. Either the phone is turned off, or the user manually turns tracking off
    tracking.
    - If the phone is turned off and turned on again, then we receive a REBOOT transition,
    and we INITIALIZE the FSM.
    - If the tracking is turned off and on manually, we should receive a STOP_TRACKING + START_TRACKING
    transition, followed by an INITIALIZE

    Should we use the special transitions or INITIALIZE? We also call INITIALIZE when we
    change the data collection config, which doesn't necessarily require a break in the chain.
    Let's use the real transitions for now.
    :param transition_df: the set of transitions for this time range
    :return: whether the tracking was restarted in that time range
    """
    restart_events_df = transition_df[(transition_df.transition == ecwt.TransitionType.BOOTED.value) |
                                      (transition_df.transition == ecwt.TransitionType.STOP_TRACKING.value)]
    if len(restart_events_df) > 0:
        logging.debug("On android, found restart events %s" % restart_events_df)
    return len(restart_events_df) > 0

def _is_tracking_restarted_ios(transition_df):
    """
    Unfortunately, life is not as simple on iOS. There is no way to sign up for a
    reboot transition. So we don't know when the phone rebooted. Instead, geofences
    are persisted across reboots. So the cases are:
    - phone runs out of battery in:
        - waiting_for_geofence:
            - we turn on phone in a different location: geofence is triggered,
             we are OUTSIDE geofence, we start tracking, we detect we are not moving,
             we turn off tracking. There doesn't appear to be anything that we can
             use here to distinguish from a spurious trip without reboot, except that
             in a spurious trip, the new location is the same while in this, it is different.
             but that is covered by the distance checks above.

           - we turn on phone in the same location: nothing to distinguish whatsoever.
             Have to let this go
        - ongoing_trip:
           - turn on as part of the same trip - will be initialized with the significant
             location changes API. no way to distinguish between significant and
             standard location changes, so it will just look like a big gap in points
             (greater than the distance filter)
           - turn on at home - barring visit notifications, will have tracking turned off
             until next trip starts and we get a significant location changes API update.
             Need to test how visit tracking interacts with all this

    Experimental results at:
    https://github.com/e-mission/e-mission-server/issues/378

    :param transition_df: the set of transitions for this time range
    :return: whether the tracking was restarted in that time range
    """
    restart_events_df = transition_df[(transition_df.transition == ecwt.TransitionType.STOP_TRACKING.value) |
                                      ((transition_df.curr_state == ecwt.State.WAITING_FOR_TRIP_START.value) &
                                       (transition_df.transition == ecwt.TransitionType.VISIT_ENDED.value))]
    if len(restart_events_df) > 0:
        logging.debug("On iOS, found restart events %s" % restart_events_df)
    return len(restart_events_df) > 0


