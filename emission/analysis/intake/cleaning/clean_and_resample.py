from __future__ import division
from __future__ import unicode_literals
from __future__ import print_function
from __future__ import absolute_import
# We need to decide whether the functions will return the cleaned entries or
# just save them directly. Returning makes it easier to test, saving makes it
# easier to code because you can work locally and just save the results.
# Right now, I'm going to save as I generate rather than return because then
# I don't have to worry about return formats etc. Although probably the return
# format is just a list of entries? Also then we only need to keep points from
# one section in memory at a time.
#
# TODO: We can revisit once we see what the structures look like.

# General imports
from future import standard_library
standard_library.install_aliases()
from builtins import zip
from builtins import *
from past.utils import old_div
import logging
import numpy as np
import pandas as pd
import arrow
import geojson as gj
import json

# Our imports
import emission.analysis.config as eac
import emission.analysis.intake.location_utils as eail
import emission.analysis.intake.domain_assumptions as eaid

import emission.storage.decorations.timeline as esdtl
import emission.storage.decorations.trip_queries as esdtq
import emission.storage.decorations.analysis_timeseries_queries as esda
import emission.storage.decorations.local_date_queries as esdl
import emission.storage.decorations.place_queries as esdp

import emission.storage.timeseries.abstract_timeseries as esta

import emission.storage.pipeline_queries as epq

import emission.analysis.intake.cleaning.location_smoothing as eaicl

import emission.core.wrapper.entry as ecwe
import emission.core.wrapper.cleanedtrip as ecwct
import emission.core.wrapper.cleanedplace as ecwcp
import emission.core.wrapper.cleanedsection as ecwcs
import emission.core.wrapper.stop as ecwst
import emission.core.wrapper.location as ecwl
import emission.core.wrapper.recreatedlocation as ecwrl
import emission.core.wrapper.untrackedtime as ecwut
import emission.core.wrapper.motionactivity as ecwm

import emission.core.common as ecc

import emission.net.ext_service.geocoder.nominatim as eco

import attrdict as ad

filtered_trip_excluded = ["start_place", "end_place",
                          "start_ts", "start_fmt_time", "start_local_dt", "start_loc",
                          "duration", "distance", "_id"]
filtered_untracked_excluded = ["start_place", "end_place", "_id"]
# We are not copying over any of the exit information from the raw place because
# we may want to squish a bunch of places together, and then the end information
# will come from the final squished place
filtered_place_excluded = ["exit_ts", "exit_local_dt", "exit_fmt_time",
                           "starting_trip", "ending_trip", "duration", "_id"]
filtered_section_excluded = ["trip_id", "start_stop", "end_stop", "distance", "duration",
                             "start_ts", "start_fmt_time", "start_local_dt","start_loc",
                             "end_ts", "end_fmt_time", "end_local_dt", "end_loc",
                             "sensed_mode", "_id"]
filtered_stop_excluded = ["trip_id", "ending_section", "starting_section",
                          "enter_ts", "enter_fmt_time", "enter_local_dt", "enter_loc",
                          "exit_ts", "exit_fmt_time", "exit_local_dt", "exit_loc",
                          "duration", "distance", "_id"]
filtered_location_excluded = ["speed", "distance", "_id"]
extrapolated_location_excluded = ["ts", "fmt_time", "local_dt", "_id"]

def clean_and_resample(user_id):
    time_query = epq.get_time_range_for_clean_resampling(user_id)
    try:
        last_raw_place = save_cleaned_segments_for_ts(user_id, time_query.startTs, time_query.endTs)
        epq.mark_clean_resampling_done(user_id, last_raw_place)
    except:
        logging.exception("Cleaning and resampling failed for user %s" % user_id)
        epq.mark_clean_resampling_failed(user_id)

def save_cleaned_segments_for_ts(user_id, start_ts, end_ts):
    """
    Take an unfiltered timeline and filter it by:
    - Removing trip_entries with no section_entries (typically caused by erroneous geofence exits)
    - Merging places on the two sides of a removed trip
    - Removing outlier points
    - Resampling at a fixed frequency, so that the heatmaps are consistent even
      if the raw data was sampled at different frequencies
    - Joining section_entries to each other by adding "transition" section_entries
    """
    tl = esdtl.get_raw_timeline(user_id, start_ts, end_ts)
    tl.fill_start_end_places()
    # For really old users who have data stored in the old format, or for
    # really new users who have no data yet, we may have no data in the
    # timeline, and no start place. If we don't have this check, we continue
    # trying to process the information for this user and end up with 
    # `raw_place = None` in `create_and_link_timeline`
    if tl.is_empty():
        logging.info("Raw timeline is empty, early return")
        return None
    return save_cleaned_segments_for_timeline(user_id, tl)

def save_cleaned_segments_for_timeline(user_id, tl):
    ts = esta.TimeSeries.get_time_series(user_id)
    trip_map = {}
    id_or_none = lambda wrapper: wrapper.get_id() if wrapper is not None else None
    for trip in tl.trips:
        try:
            if trip.metadata.key == esda.RAW_UNTRACKED_KEY:
                filtered_trip = get_filtered_untracked(ts, trip)
            else:
                filtered_trip = get_filtered_trip(ts, trip)
            logging.debug("For raw trip %s, found filtered trip %s" %
                          (id_or_none(trip), id_or_none(filtered_trip)))
            if filtered_trip is not None:
                trip_map[trip.get_id()] = filtered_trip
        except KeyError as e:
            # We ran into key errors while dealing with mixed filter trip_entries.
            # I think those should be resolved for now, so we can raise the error again
            # But if this is preventing us from making progress, we can comment out the raise
            logging.exception("Found key error %s while processing trip %s" % (e, trip))
            # raise e
        except Exception as e:
            logging.exception("Found error %s while processing trip %s" % (e, trip))
            raise e

    (last_cleaned_place, filtered_tl) = create_and_link_timeline(tl, user_id, trip_map)

    # We have updated the first place entry in the filtered_tl, but everything
    # else is new and needs to be inserted
    if last_cleaned_place is not None:
        logging.debug("last cleaned_place %s was already in database, updating..." % 
            last_cleaned_place)
        ts.update(last_cleaned_place)
    if filtered_tl is not None and not filtered_tl.is_empty():
        ts.bulk_insert(list(filtered_tl), esta.EntryType.ANALYSIS_TYPE)
    return tl.last_place()

def get_filtered_untracked(ts, untracked):
    # untracked time is very simple, but we need to map them in a basic way to avoid
    # holes in the timeline
    logging.debug("Found untracked entry %s (%s -> %s), skipping processing" %
                  (untracked.get_id(), untracked.data.start_fmt_time, untracked.data.end_fmt_time))
    filtered_untracked_data = ecwut.Untrackedtime()
    _copy_non_excluded(old_data=untracked.data,
                       new_data=filtered_untracked_data,
                       excluded_list=filtered_untracked_excluded)
    filtered_untracked_entry = ecwe.Entry.create_entry(untracked.user_id, esda.CLEANED_UNTRACKED_KEY,
                                                  filtered_untracked_data,
                                                  create_id = True)
    return filtered_untracked_entry

def get_filtered_trip(ts, trip):
    logging.debug("Filtering trip %s" % trip)
    trip_tl = esdtq.get_raw_timeline_for_trip(trip.user_id, trip.get_id())
    # trip_tl is the timeline for this particular trip, which contains the
    # section_entries and trip_entries
    if len(trip_tl.trips) == 0:
        logging.info("Found zero section trip %s " % trip)
        return None

    # Else, this is a non-zero section trip
    filtered_trip_data = ecwct.Cleanedtrip()

    _copy_non_excluded(old_data=trip.data,
                       new_data=filtered_trip_data,
                       excluded_list=filtered_trip_excluded)

    filtered_trip_data.raw_trip = trip.get_id()
    filtered_trip_entry = ecwe.Entry.create_entry(trip.user_id, esda.CLEANED_TRIP_KEY,
                                                  filtered_trip_data,
                                                  create_id = True)

    # Map from old section id -> new section object
    section_map = {}
    point_map = {}
    for section in trip_tl.trips:
        (filtered_section_entry, point_list) = get_filtered_section(filtered_trip_entry, section)
        section_map[section.get_id()] = filtered_section_entry
        point_map[section.get_id()] = point_list

    # Handle the stops first because changing the ends of the section has 
    # implications for all the manipulations on the section
    stop_map = {}
    for stop in trip_tl.places:
        stop_map[stop.get_id()] = get_filtered_stop(filtered_trip_entry, stop)

    # Set the start point and time for the trip from the start point and time
    # from the first section
    # section map values are (section, points_map) tuples
    first_cleaned_section = section_map[trip_tl.trips[0].get_id()]
    last_cleaned_section = section_map[trip_tl.trips[-1].get_id()]
    logging.debug("Copying start_ts %s, start_fmt_time %s, start_local_dt from %s to %s" %
                  (first_cleaned_section.data.start_ts, first_cleaned_section.data.start_fmt_time,
                   first_cleaned_section.get_id(), filtered_trip_data))
    _set_extrapolated_start_for_trip(filtered_trip_data, first_cleaned_section, last_cleaned_section)

    # After we have linked everything back together. NOW we can save the entries
    linked_tl = link_trip_timeline(trip_tl, section_map, stop_map)

    # We potentially overwrite the stop distance as part of the linking,
    # as we copy over values from the underlying section. Since the trip
    # distance includes the stop distance, we need to compute the trip distance
    # after computing the stop distance.
    trip_distance = sum([section.data.distance for section in list(section_map.values())]) + \
                    sum([stop.data.distance for stop in list(stop_map.values())])
    filtered_trip_data.distance = trip_distance
    filtered_trip_entry["data"] = filtered_trip_data
    if filtered_trip_data.distance < 100:
        logging.info("Skipped single point trip %s (%s -> %s) of length %s" %
                     (trip.get_id(), trip.data.start_fmt_time,
                      trip.data.end_fmt_time, filtered_trip_data.distance))
        return None

    # But then we want to check the stop distance to determine whether the trip is valid
    # or not, and to not store any of the sections or stops if it is not. So the validity
    # check should be before the insert

    for section_id, points in list(point_map.items()):
        # We should have filtered out zero point sections already
        logging.debug("About to store %s points for section %s" %
                      (len(points), section_id))
        if len(points) > 0:
            ts.bulk_insert(points, esta.EntryType.ANALYSIS_TYPE)

    if not linked_tl.is_empty():
        ts.bulk_insert(list(linked_tl), esta.EntryType.ANALYSIS_TYPE)
    return filtered_trip_entry

def get_filtered_place(raw_place):
    filtered_place_data = ecwcp.Cleanedplace()

    _copy_non_excluded(old_data=raw_place.data,
                       new_data=filtered_place_data,
                       excluded_list=filtered_place_excluded)

    filtered_place_data.raw_places = []
    filtered_place_data.append_raw_place(raw_place.get_id())

    try:
        reverse_geocoded_json = eco.Geocoder.get_json_reverse(filtered_place_data.location.coordinates[1],
                                                              filtered_place_data.location.coordinates[0])
        if reverse_geocoded_json is not None:
            filtered_place_data.display_name = format_result(reverse_geocoded_json)
    except KeyError as e:
        logging.info("nominatim result does not have an address, skipping")
        logging.info(e)
    except:
        logging.info("Unable to pre-fill reverse geocoded information, client has to do it")

    logging.debug("raw_place.user_id = %s" % raw_place.user_id)
    curr_cleaned_end_place = ecwe.Entry.create_entry(raw_place.user_id,
                                                     esda.CLEANED_PLACE_KEY,
                                                     filtered_place_data,
                                                     create_id=True)
    return curr_cleaned_end_place

def get_filtered_section(new_trip_entry, section):
    """
    Save the filtered points associated with this section.
    Do I need a cleaned section as well?
    At first glance, it doesn't seem necessary, since all sections are valid.
    But don't we need a cleaned section that has a trip id == the cleaned trip.
    Otherwise, we need to modify the trip_id of the raw section, which means
    that this is no longer a progressive system that we can recreate.
    So let's create a new entry for the cleaned section that is identical to
    the old section, but pointing to the cleaned trip.
    :param ts: the timeseries for this user
    :param trip: the trip that this section is a part of
    :param section: the section that we are saving
    :return: None
    """
    filtered_section_data = ecwcs.Cleanedsection()

    _copy_non_excluded(old_data=section.data,
                       new_data=filtered_section_data,
                       excluded_list=filtered_section_excluded)

    filtered_section_data.trip_id = new_trip_entry.get_id()

    # Now that we have constructed the section object, we have to filter and
    # store the related data points
    with_speeds_df = get_filtered_points(section, filtered_section_data)
    speeds = list(with_speeds_df.speed)
    distances = list(with_speeds_df.distance)
    filtered_section_data.speeds = [float(s) for s in speeds]
    filtered_section_data.distances = [float(d) for d in distances]
    filtered_section_data.distance = float(sum(distances))

    overridden_mode = get_overriden_mode(section.data, filtered_section_data, with_speeds_df)
    if overridden_mode is not None:
        filtered_section_data.sensed_mode = overridden_mode
    else:
        filtered_section_data.sensed_mode = section.data.sensed_mode

    filtered_section_entry = ecwe.Entry.create_entry(section.user_id,
                                    esda.CLEANED_SECTION_KEY,
                                    filtered_section_data, create_id=True)

    ts = esta.TimeSeries.get_time_series(section.user_id)
    entry_list = []
    for row in with_speeds_df.to_dict('records'):
        loc_row = ecwrl.Recreatedlocation(row)
        loc_row.mode = section.data.sensed_mode
        loc_row.section = filtered_section_entry.get_id()
        entry_list.append(ecwe.Entry.create_entry(section.user_id, esda.CLEANED_LOCATION_KEY, loc_row))

    return (filtered_section_entry, entry_list)

def get_filtered_stop(new_trip_entry, stop):
    """
    If we have filtered sections, I guess we need to have filtered stops as well.
    These should also point to the new trip

    :param new_trip_trip: the trip that this stop is part of
    :param stop: the section that this is part of
    :return: None
    """
    filtered_stop_data = ecwst.Stop()
    filtered_stop_data.trip_id = new_trip_entry.get_id()

    _copy_non_excluded(old_data=stop.data,
                       new_data=filtered_stop_data,
                       excluded_list=filtered_stop_excluded)

    return ecwe.Entry.create_entry(stop.user_id, esda.CLEANED_STOP_KEY,
                                   filtered_stop_data, create_id=True)

def get_filtered_points(section, filtered_section_data):
    logging.debug("Getting filtered points for section %s" % section)
    logging.debug("Saving entries into cleaned section %s" % filtered_section_data)
    ts = esta.TimeSeries.get_time_series(section.user_id)
    loc_entry_it = ts.find_entries(["background/filtered_location"],
                                   esda.get_time_query_for_trip_like(
                                       esda.RAW_SECTION_KEY, section.get_id()))

    loc_entry_list = [ecwe.Entry(e) for e in loc_entry_it]

    # We know that the assertion fails in the geojson conversion code and we
    # handle it there, so we are just going to comment this out for now.
    # assert (loc_entry_list[-1].data.loc == section.data.end_loc,
    #         "section_location_array[-1].loc != section.end_loc even after df.ts fix",
    #         (loc_entry_list[-1].data.loc, section.data.end_loc))

    # Find the list of points to filter
    filtered_points_entry_doc = ts.get_entry_at_ts("analysis/smoothing",
                                                   "data.section",
                                                   section.get_id())

    if filtered_points_entry_doc is None:
        logging.debug("No filtered_points_entry, filtered_points_list is empty")
        filtered_point_id_list = []
    else:
        # TODO: Figure out how to make collections work for the wrappers and then change this to an Entry
        filtered_points_entry = ad.AttrDict(filtered_points_entry_doc)
        filtered_point_id_list = list(filtered_points_entry.data.deleted_points)
        logging.debug("deleting %s points from section points" % len(
            filtered_point_id_list))

    filtered_loc_df = remove_outliers(loc_entry_list, filtered_point_id_list)
    if len(filtered_loc_df) == 0:
        import emission.storage.timeseries.builtin_timeseries as estb

        logging.info("Removed all locations from the list."
            "Setting cleaned start + end = raw section start, so effectively zero distance")
        section_start_loc = ts.get_entry_at_ts("background/filtered_location",
                                               "data.ts", section.data.start_ts)
        section_start_loc_df = ts.to_data_df("background/filtered_location", [section_start_loc])
        logging.debug("section_start_loc_df = %s" % section_start_loc_df.iloc[0])
        _set_extrapolated_vals_for_section(filtered_section_data,
                                           section_start_loc_df.iloc[0],
                                           section_start_loc_df.iloc[-1])
        with_speeds_df = eaicl.add_dist_heading_speed(filtered_loc_df).dropna()
        logging.info("Early return with df = %s" % with_speeds_df)
        return with_speeds_df

    if section.data.start_stop is None:
        logging.debug("Found first section, may need to extrapolate start point")
        raw_trip = ts.get_entry_from_id(esda.RAW_TRIP_KEY, section.data.trip_id)
        raw_start_place = ts.get_entry_from_id(esda.RAW_PLACE_KEY, raw_trip.data.start_place)
        if not is_place_bogus(loc_entry_list, 0, filtered_point_id_list, raw_start_place):
            filtered_loc_df = _add_start_point(filtered_loc_df, raw_start_place, ts, section.data.sensed_mode)
            if _is_unknown_mark_needed(filtered_section_data, section, filtered_loc_df):
                # UGLY! UGLY! Fix when we fix
                # https://github.com/e-mission/e-mission-server/issues/388
                section["data"]["sensed_mode"] = ecwm.MotionTypes.UNKNOWN

    if section.data.end_stop is None:
        logging.debug("Found last section, may need to extrapolate end point")
        raw_trip = ts.get_entry_from_id(esda.RAW_TRIP_KEY, section.data.trip_id)
        raw_end_place = ts.get_entry_from_id(esda.RAW_PLACE_KEY, raw_trip.data.end_place)
        if not is_place_bogus(loc_entry_list, -1, filtered_point_id_list, raw_end_place):
            filtered_loc_df = _add_end_point(filtered_loc_df, raw_end_place, ts)

    # Can move this up to get_filtered_section if we want to ensure that we
    # don't touch filtered_section_data in here
    logging.debug("Setting section start and end values of %s to first point %s"
                  "and last point %s" %
                  (filtered_section_data, filtered_loc_df.iloc[0], filtered_loc_df.iloc[-1]))
    _set_extrapolated_vals_for_section(filtered_section_data,
                                        filtered_loc_df.iloc[0],
                                        filtered_loc_df.iloc[-1])

    # if section.data.start_stop is None:
    #     first_point = filtered_loc_list.pop(0)
    #     logging.debug("first section - removing %s to avoid badly interpolated results" % first_point)

    logging.debug("Resampling data of size %s" % len(filtered_loc_df))
    # filtered_loc_list has removed the outliers. Now, we resample the data at
    # 30 sec intervals

    """
    There is a problem with working on this on a section by section basis.
    Namely, how do we deal with the ends. In particular, note that our sections
    are generated by looking at motion activity while our points are generated
    by looking at locations, and the two streams are not synchronized. So we have
    "transition areas" between sections that have no points. Resampling will
    help with this because we can ensure that arbitrary time point - for example,
    the transition between sections, has an associated location point. But that
    fix really needs to be made in the segmentation code - by this point, the
    original timestamps have been replaced by the timestamps of the start and
    end location points. But we can also have a gap if the duration of the section
    is not a multiple of the interval, as it is likely to be. In this case, we will
    manually add the last entry to ensure that we adequately capture the end point.
    :param filtered_loc_df:
    :param interval:
    :return:
    """
    resampled_loc_df = eail.resample(filtered_loc_df, interval=30)

    with_speeds_df = eaicl.add_dist_heading_speed(resampled_loc_df)
    with_speeds_df["idx"] = np.arange(0, len(with_speeds_df))
    logging.debug("with_speeds_df = %s" %
                  with_speeds_df[["idx", "ts", "fmt_time", "longitude", "latitude"]].head())
    with_speeds_df_nona = with_speeds_df.dropna()
    logging.info("removed %d entries containing n/a" % 
        (len(with_speeds_df_nona) - len(with_speeds_df)))
    logging.debug("get_filtered_points(%s points) = %s points" %
                  (len(loc_entry_list), len(with_speeds_df_nona)))
    return with_speeds_df_nona

def is_place_bogus(loc_entry_list, loc_index, filtered_point_id_list, raw_start_place):
    curr_loc = loc_entry_list[loc_index]
    logging.debug("At index %s, loc is %s" % (loc_index, curr_loc.get_id()))
    if curr_loc.get_id() in filtered_point_id_list:
        logging.debug("First point %s (%s) was filtered, raw_start_place %s (%s) may be bogus" %
                      (curr_loc.get_id(), curr_loc.data.loc.coordinates,
                      raw_start_place.get_id(), raw_start_place.data.location.coordinates))
        place_to_point_dist = ecc.calDistance(curr_loc.data.loc.coordinates,
                                              raw_start_place.data.location.coordinates)
        # If the start place is also bogus, no point in joining to it
        if place_to_point_dist < 100:
            logging.debug("place_to_point_dist = %s, previous place is also bogus, skipping extrapolation" %
                          place_to_point_dist)
            return True
    #else
    return False

def _is_unknown_mark_needed(filtered_section_data, section, filtered_loc_df):
    import emission.analysis.intake.cleaning.cleaning_methods.speed_outlier_detection as eaiccs

    if filtered_loc_df.ts.iloc[0] == section.data.start_ts:
        logging.debug("No extrapolation, no UNKNOWN")
        return False

    with_speeds_df = eaicl.add_dist_heading_speed(filtered_loc_df)
    logging.debug("with_speeds_df = %s" % (with_speeds_df[["fmt_time", "ts",
                           "latitude", "longitude", "speed", "distance"]].head()))

    extrapolated_speed = with_speeds_df.speed.iloc[1]
    remaining_speeds = with_speeds_df.iloc[2:]
    threshold = eaiccs.BoxplotOutlier(eaiccs.BoxplotOutlier.MINOR,
                          ignore_zeros=False).get_threshold(remaining_speeds)

    if extrapolated_speed > threshold:
        logging.debug("extrapolated_speed %s > threshold %s, marking section as UNKNOWN" %
                      (extrapolated_speed, threshold))
        return True
    else:
        logging.debug("extrapolated_speed %s < threshold %s, leaving section untouched" %
                      (extrapolated_speed, threshold))
        return False

def _copy_non_excluded(old_data, new_data, excluded_list):
    for key in old_data:
        if key not in excluded_list:
            new_data[key] = old_data[key]

def get_overriden_mode(raw_section_data, filtered_section_data, with_speeds_df):
    end_to_end_distance = filtered_section_data.distance
    end_to_end_time = filtered_section_data.duration

    if (end_to_end_distance == 0) or (end_to_end_time == 0):
        logging.info("distance = time = 0 for section in trip (raw: %s, cleaned %s), returning None" % 
        (raw_section_data.trip_id, filtered_section_data.trip_id))
        return None

    if is_air_section(filtered_section_data, with_speeds_df):
        return ecwm.MotionTypes.AIR_OR_HSR

    overall_speed = old_div(end_to_end_distance, end_to_end_time)
    TEN_KMPH = old_div(float(10 * 1000), (60 * 60)) # m/s
    TWENTY_KMPH = old_div(float(20 * 1000), (60 * 60)) # m/s
    logging.debug("end_to_end_distance = %s, end_to_end_time = %s, overall_speed = %s" %
                  (end_to_end_distance, end_to_end_time, overall_speed))

    # Hardcoded hack as per
    # https://github.com/e-mission/e-mission-server/issues/407#issuecomment-248524098
    if raw_section_data.sensed_mode == ecwm.MotionTypes.ON_FOOT:
        if end_to_end_distance > 10 * 1000 and overall_speed > TEN_KMPH:
            logging.info("Sanity checking failed for ON_FOOT section from trip (raw: %s, filtered %s), returning UNKNOWN" % 
                (raw_section_data.trip_id, filtered_section_data.trip_id))
            return ecwm.MotionTypes.UNKNOWN

    if raw_section_data.sensed_mode == ecwm.MotionTypes.BICYCLING:
        if end_to_end_distance > 100 * 1000 and overall_speed > TWENTY_KMPH:
            logging.info("Sanity checking failed for BICYCLING section from trip (raw: %s, filtered %s), returning UNKNOWN" % 
                (raw_section_data.trip_id, filtered_section_data.trip_id))
            return ecwm.MotionTypes.UNKNOWN

    return None

def is_air_section(filtered_section_data,with_speeds_df):
    HUNDRED_KMPH = old_div(float(100 * 1000), (60 * 60)) # m/s
    ONE_FIFTY_KMPH = old_div(float(100 * 1000), (60 * 60)) # m/s
    end_to_end_distance = filtered_section_data.distance
    end_to_end_time = filtered_section_data.duration
    end_to_end_speed = old_div(end_to_end_distance, end_to_end_time)
    logging.debug("air check: end_to_end_distance = %s, end_to_end_time = %s, so end_to_end_speed = %s" %
                  (end_to_end_distance, end_to_end_time, end_to_end_speed))
    if end_to_end_speed > ONE_FIFTY_KMPH:
        logging.debug("air check: end_to_end_speed %s > ONE_FIFTY_KMPH %s, returning True " %
                      (end_to_end_speed, ONE_FIFTY_KMPH))
        return True

    logging.debug("first check failed, speed distribution is %s" %
                  with_speeds_df.speed.describe(percentiles=[0.9,0.95,0.97,0.99]))

    if end_to_end_speed > HUNDRED_KMPH and \
        with_speeds_df.speed.quantile(0.9) > ONE_FIFTY_KMPH:
        logging.debug("air check: end_to_end_speed %s > HUNDRED_KMPH %s, and 0.9 percentile %s > ONE_FIFTY_KMPH %s, returning True " %
                      (end_to_end_speed, HUNDRED_KMPH,
                       with_speeds_df.speed.quantile(0.9), ONE_FIFTY_KMPH))
        return True

    logging.debug("air check: end_to_end_speed %s < HUNDRED_KMPH %s or"
                  "0.9 percentile %s < ONE_FIFTY_KMPH %s, returning False" %
                  (end_to_end_speed, HUNDRED_KMPH,
                   with_speeds_df.speed.quantile(0.9), ONE_FIFTY_KMPH))
    return False

def _add_start_point(filtered_loc_df, raw_start_place, ts, sensed_mode):
    raw_start_place_enter_loc_entry = _get_raw_place_enter_loc_entry(ts, raw_start_place)

    curr_first_point = filtered_loc_df.iloc[0]
    curr_first_loc = gj.GeoJSON.to_instance(curr_first_point["loc"])
    add_dist = ecc.calDistance(curr_first_loc.coordinates,
                               raw_start_place_enter_loc_entry.data.loc.coordinates)
    with_speeds_df = eaicl.add_dist_heading_speed(filtered_loc_df)

    logging.debug("Adding distance %s to original %s to extend section start from %s to %s" %
                  (add_dist, with_speeds_df.distance.sum(),
                   curr_first_loc.coordinates, raw_start_place_enter_loc_entry.data.loc.coordinates))
    # speed is in m/s. We want to compute secs for covering ad meters
    # speed m = 1 sec, ad m = ? ad/speed secs
    if with_speeds_df.speed.median() > 0:
        if add_dist > 2 * with_speeds_df.distance.sum() \
            and not eaid.is_motorized(sensed_mode):
            # Better handling of iOS small non-motorized trips
            # https://github.com/e-mission/e-mission-server/issues/577#issuecomment-379496118
            computed_median = with_speeds_df.speed.median()
            if (eaid.is_walking_type(sensed_mode)):
                speed_fn = eaid.is_walking_speed
            else:
                speed_fn = eaid.is_bicycling_speed
            logging.debug("motorized? %s, speed unchanged %s, speed reduced %s" %
                        (eaid.is_motorized(sensed_mode),
                        speed_fn(computed_median),
                        speed_fn(0.9 * computed_median)))
            if not speed_fn(computed_median) and speed_fn(0.9 * computed_median):
                del_time = add_dist / speed_fn(0.9 * computed_median)
                logging.info("median speeds for %s section is %s, resetting to %s instead" %
                    (sensed_mode, computed_median, 0.9 * computed_median))
            else:
                del_time = add_dist / computed_median
        else:
            del_time = add_dist / with_speeds_df.speed.median()
    else:
        logging.info("speeds for this section are %s, median is %s, trying median nonzero instead" %
                     (with_speeds_df.speed, with_speeds_df.speed.median()))
        speed_nonzero = (with_speeds_df.speed[with_speeds_df.speed != 0])
        if not np.isnan(speed_nonzero.median()):
            logging.info("nonzero speeds = %s, median is %s" %
                         (speed_nonzero, speed_nonzero.median()))
            del_time = add_dist / speed_nonzero.median()
        else:
            logging.info("non_zero speeds = %s, median is %s, unsure what this even means, skipping" %
                         (speed_nonzero, speed_nonzero.median()))
            del_time = 0


    if del_time == 0:
        logging.debug("curr_first_point %s, %s == start_place exit %s, %s"
                      "skipping add of first point" %
                      (curr_first_point.fmt_time, curr_first_point["loc"]["coordinates"],
                       raw_start_place.data.exit_fmt_time, raw_start_place.data.location.coordinates))
        return filtered_loc_df

    new_start_ts = curr_first_point.ts - del_time
    prev_enter_ts = raw_start_place.data.enter_ts if "enter_ts" in raw_start_place.data else None

    if prev_enter_ts is not None and new_start_ts < raw_start_place.data.enter_ts:
        logging.debug("Much extrapolation! new_start_ts %s < prev_enter_ts %s" %
                      (new_start_ts, prev_enter_ts))
        if raw_start_place.data.duration == 0:
            ts = esta.TimeSeries.get_time_series(raw_start_place.user_id)
            ending_trip_entry = ecwe.Entry(
                ts.get_entry_from_id(esda.RAW_TRIP_KEY,
                                     raw_start_place.data.ending_trip))
            if ending_trip_entry is None or ending_trip_entry.metadata.key != "segmentation/raw_untracked":
                logging.debug("place %s has zero duration but is after %s!" % 
                             (raw_start_place.get_id(), ending_trip_entry))
                assert False
            else:
                logging.debug("place %s is after untracked_time %s, has zero duration!" %
                             (raw_start_place.get_id(), ending_trip_entry.get_id()))
            new_start_ts = raw_start_place.data.enter_ts
        else:
            new_start_ts = min(raw_start_place.data.enter_ts + old_div(raw_start_place.data.duration, 2),
                                  raw_start_place.data.enter_ts + 3 * 60)

        logging.debug("changed new_start_ts to %s" % (new_start_ts))

    logging.debug("After subtracting time %s from original %s to cover additional distance %s at speed %s, new_start_ts = %s" %
                  (del_time, filtered_loc_df.ts.iloc[-1] - filtered_loc_df.ts.iloc[0],
                   add_dist, with_speeds_df.speed.median(), new_start_ts))

    new_first_point_data = ecwl.Location()
    _copy_non_excluded(old_data=raw_start_place_enter_loc_entry.data,
                       new_data=new_first_point_data,
                       excluded_list=extrapolated_location_excluded)

    logging.debug("After copy, new data = %s" % new_first_point_data)
    new_first_point_data["ts"] = new_start_ts
    tz = raw_start_place_enter_loc_entry.data.local_dt.timezone
    new_first_point_data["local_dt"] = esdl.get_local_date(new_start_ts, tz)
    new_first_point_data["fmt_time"] = arrow.get(new_start_ts).to(tz).isoformat()
    new_first_point = ecwe.Entry.create_entry(curr_first_point.user_id,
                            "background/filtered_location",
                            new_first_point_data)
    return _insert_new_entry(-1, filtered_loc_df, new_first_point)

def _add_end_point(filtered_loc_df, raw_end_place, ts):
    raw_end_place_enter_loc_entry = _get_raw_place_enter_loc_entry(ts, raw_end_place)
    curr_last_point = filtered_loc_df.iloc[-1]
    curr_last_loc = gj.GeoJSON.to_instance(curr_last_point["loc"])
    add_dist = ecc.calDistance(curr_last_loc.coordinates,
                               raw_end_place_enter_loc_entry.data.loc.coordinates)
    if add_dist == 0:
        logging.debug("section end %s = place enter %s, nothing to fix" %
                      (curr_last_loc, raw_end_place_enter_loc_entry))
        return filtered_loc_df

    # Note that, unlike _add_start_point, we don't need to extrapolate the time here
    # we know the time at which the next place was entered, and we have the entry
    # right here. In particular, if the enter information is missing (e.g. at the
    # start of a chain, we won't have any trip/section before it, and won't try
    # to extrapolate to the place

    # because the enter_ts is None
    assert(raw_end_place.data.enter_ts is not None)
    logging.debug("Found mismatch of %s in last section %s -> %s, "
                   "appending location %s, %s, %s to fill the gap" %
                  (add_dist, curr_last_loc.coordinates, raw_end_place_enter_loc_entry.data.loc.coordinates,
                   raw_end_place_enter_loc_entry.data.loc.coordinates, raw_end_place_enter_loc_entry.data.fmt_time, raw_end_place_enter_loc_entry.data.ts))
    return _insert_new_entry(filtered_loc_df.index[-1]+1, filtered_loc_df, raw_end_place_enter_loc_entry)

def _get_raw_place_enter_loc_entry(ts, raw_place):
    if raw_place.data.enter_ts is not None:
        raw_start_place_enter_loc_entry = ecwe.Entry(
            ts.get_entry_at_ts("background/filtered_location", "data.ts",
                               raw_place.data.enter_ts))
    else:
        # These are not strictly accurate because the exit ts for the place
        # corresponds to the ts of the first point in the section. We are trying
        # to determine the correct exit_ts here. But its a reasonable estimate,
        # at least for the time zone, which is required when we extrapolate
        # note that this will fail for the specific case in which the first point outside
        # the geofence of the first place in a trip chain is in a different timezone
        # than the point itself. We can work around that by storing the enter_ts even
        # for the first place.
        dummy_section_start_loc_doc = {
            "loc": raw_place.data.location,
            "latitude": raw_place.data.location.coordinates[1],
            "longitude": raw_place.data.location.coordinates[0],
            "ts": raw_place.data.exit_ts,
            "fmt_time": raw_place.data.exit_fmt_time,
            "local_dt": raw_place.data.exit_local_dt
        }
        raw_start_place_enter_loc_entry = ecwe.Entry.create_entry(raw_place.user_id,
                                                                  "background/filtered_location",
                                                                  dummy_section_start_loc_doc)
    logging.debug("Raw place is %s and corresponding location is %s" %
                  (raw_place.get_id(), raw_start_place_enter_loc_entry.get_id()))
    return raw_start_place_enter_loc_entry

def _insert_new_entry(index, loc_df, entry):
    import emission.storage.timeseries.builtin_timeseries as estb

    new_point_row = estb.BuiltinTimeSeries._to_df_entry(entry)
    del new_point_row["_id"]
    missing_cols = set(loc_df.columns) - set(new_point_row.keys())
    logging.debug("Missing cols = %s" % missing_cols)
    for col in missing_cols:
        new_point_row[col] = 0

    extra_cols = set(new_point_row.keys()) - set(loc_df.columns)
    logging.debug("Extra cols = %s" % extra_cols)
    for ec in extra_cols:
        del new_point_row[ec]

    still_missing_cols = set(loc_df.columns) - set(new_point_row.keys())
    logging.debug("Missing cols = %s" % still_missing_cols)

    still_extra_cols = set(new_point_row.keys()) - set(loc_df.columns)
    logging.debug("Retained extra cols = %s" % still_extra_cols)

    loc_df.loc[index] = new_point_row
    appended_loc_df = loc_df.sort_index().reset_index(drop=True)
    return appended_loc_df

def _set_extrapolated_vals_for_section(filtered_section_data, fixed_start_loc, fixed_end_loc):
    _overwrite_from_loc_row(filtered_section_data, fixed_start_loc, "start")
    _overwrite_from_loc_row(filtered_section_data, fixed_end_loc, "end")
    filtered_section_data.duration = filtered_section_data.end_ts - filtered_section_data.start_ts

def _set_extrapolated_start_for_trip(filtered_trip_data, first_section, last_section):
    _copy_prefixed_fields(filtered_trip_data, "start", first_section.data, "start")
    _copy_prefixed_fields(filtered_trip_data, "end", last_section.data, "end")
    filtered_trip_data.duration = filtered_trip_data.end_ts - filtered_trip_data.start_ts

def _overwrite_from_loc_row(filtered_section_data, fixed_loc, prefix):
    _overwrite_from_timestamp(filtered_section_data, prefix,
                              fixed_loc.ts, fixed_loc.local_dt_timezone,
                              fixed_loc["loc"])

def _overwrite_from_timestamp(filtered_trip_like, prefix, ts, tz, loc):
    filtered_trip_like[prefix+"_ts"] = float(ts)
    filtered_trip_like[prefix+"_local_dt"] = esdl.get_local_date(ts, tz)
    filtered_trip_like[prefix+"_fmt_time"] = arrow.get(ts).to(tz).isoformat()
    filtered_trip_like[prefix+"_loc"] = loc

def remove_outliers(raw_loc_entry_list, filtered_point_id_list):
    import emission.storage.timeseries.builtin_timeseries as estb

    loc_df = estb.BuiltinTimeSeries.to_data_df("background/filtered_location",
                                               raw_loc_entry_list)
    valid_loc_df = loc_df[np.logical_not(loc_df._id.isin(filtered_point_id_list))]
    cols_to_drop = set(filtered_location_excluded).intersection(set(loc_df.columns))
    logging.debug("from %s, excluding %s, columns to drop = %s" %
                  (loc_df.columns, filtered_location_excluded, cols_to_drop))
    filtered_loc_df = valid_loc_df.drop(cols_to_drop, axis=1)
    logging.debug("After filtering, rows went from %s -> %s, cols from %s -> %s" %
                  (len(loc_df), len(valid_loc_df),
                   len(loc_df.columns), len(filtered_loc_df.columns)))
    return filtered_loc_df

def link_trip_timeline(tl, section_map, stop_map):
    filled_in_sections = [_fill_section(s, section_map[s.get_id()], stop_map) for s in tl.trips]
    filled_in_stops = [_fill_stop(s, stop_map[s.get_id()], section_map) for s in tl.places]
    return esdtl.Timeline(esda.CLEANED_STOP_KEY, esda.CLEANED_SECTION_KEY,
                          filled_in_stops, filled_in_sections)

def _fill_section(old_section, new_section, stop_map):
    section_data = new_section.data
    # the first section will have the start_stop == None and the last section
    # will have the end_stop == None, so we handle those by a simple check
    if old_section.data.start_stop is not None:
        section_data.start_stop = stop_map[old_section.data.start_stop].get_id()
    if old_section.data.end_stop is not None:
        section_data.end_stop = stop_map[old_section.data.end_stop].get_id()
    new_section["data"] = section_data
    return new_section

def _fill_stop(old_stop, new_stop, section_map):
    stop_data = new_stop.data
    new_ending_section = section_map[old_stop.data.ending_section]
    stop_data.ending_section = new_ending_section.get_id()

    new_starting_section = section_map[old_stop.data.starting_section]
    stop_data.starting_section = new_starting_section.get_id()
    # We filter section points in this step, so it is possible to filter the first
    # or last point as well. And if we do, then the start/stop position of the stop,
    # which was linked to the section. Since we are working outward from sections,
    # and we haven't done any processing on stops, lets just set the stop values
    # to the section values
    _copy_prefixed_fields(stop_data, "enter", new_ending_section.data, "end")
    _copy_prefixed_fields(stop_data, "exit", new_starting_section.data, "start")

    stop_data.duration = stop_data.exit_ts - stop_data.enter_ts
    stop_data.distance = ecc.calDistance(stop_data.exit_loc.coordinates,
                                         stop_data.enter_loc.coordinates)

    # squish stop if necessary
    STOP_DISTANCE_THRESHOLD = max(eac.get_config()["section.startStopRadius"],
        eac.get_config()["section.endStopRadius"])
    if stop_data.distance > STOP_DISTANCE_THRESHOLD:
        logging.debug("stop distance = %d > %d, squishing it between %s -> %s and %s -> %s" % 
            (stop_data.distance, STOP_DISTANCE_THRESHOLD,
             new_ending_section.data.start_fmt_time, new_ending_section.data.end_fmt_time,
             new_starting_section.data.start_fmt_time, new_starting_section.data.end_fmt_time))
        squish_stop(stop_data, new_ending_section, new_starting_section)
        logging.debug("after squish_stop, sequence is section %s -> %s, section %s -> %s, and %s -> %s" % 
            (new_ending_section.data.start_fmt_time, new_ending_section.data.end_fmt_time,
             stop_data.enter_fmt_time, stop_data.exit_fmt_time,
             new_starting_section.data.start_fmt_time, new_starting_section.data.end_fmt_time))

    new_stop["data"] = stop_data
    return new_stop

def squish_stop(filtered_stop_data, new_ending_section, new_starting_section):
    # We will shrink the stop by expanding either the earlier or later sections
    # but which to expand.
    # we want to expand the poor quality one, since we have good data from the good
    # quality one and only speculation on the poor quality side
    #
    # How do we determine which side is better quality?
    # - non-motorized side is generally better quality
    # - side with high density of points is better quality
    # let's start with the second check

    ts = esta.TimeSeries.get_time_series(new_ending_section.user_id)
    prev_section_points_df = ts.get_data_df("background/filtered_location",
                                   esda.get_time_query_for_trip_like_object(
                                   new_ending_section.data))
    next_section_points_df = ts.get_data_df("background/filtered_location",
                                   esda.get_time_query_for_trip_like_object(
                                   new_starting_section.data))

    prev_section_space_density = len(prev_section_points_df)/(new_ending_section.data.distance + 1)
    next_section_space_density = len(next_section_points_df)/(new_starting_section.data.distance + 1)

    prev_section_time_density = len(prev_section_points_df)/(new_ending_section.data.duration + 1)
    next_section_time_density = len(next_section_points_df)/(new_starting_section.data.duration + 1)

    # Get the deltas before overwriting so that we can adjust section dist and time
    # accordingly.
    # no matter which way we move, we are going to increase the distance and
    # time of the chosen section by the same amount
    dist_diff = ecc.calDistance(filtered_stop_data.enter_loc.coordinates,
                                filtered_stop_data.exit_loc.coordinates)
    ts_diff = filtered_stop_data.enter_ts - filtered_stop_data.exit_ts

    if prev_section_space_density > next_section_space_density:
        if prev_section_time_density <= next_section_time_density:
                logging.warning("%s -> %s v/s %s -> %s, space density is (%4f, %4f) but time density is (%4f, %4f)" %
                    (new_ending_section.data.start_fmt_time, new_ending_section.data.end_fmt_time,
                     new_starting_section.data.start_fmt_time, new_starting_section.data.end_fmt_time,
                     prev_section_space_density, next_section_space_density,
                     prev_section_time_density, next_section_time_density))

        logging.debug("prev_section %s is more dense than next_section %s, merging backwards" % 
            (new_ending_section.data.end_fmt_time, new_starting_section.data.start_fmt_time))
        # NOTE: copy_prefixed_fields(a, b) is the equivalent of a = b. a is changed!!

        # merge backward. next section takes over the stop (e.g. walking ->
        # transit). So the exit should be overwritten with the enter
        _copy_prefixed_fields(filtered_stop_data, "exit", filtered_stop_data, "enter")

        # Then, the next section's start should be overwritten with this stop's
        # information (since enter now = exit, which set of fields is a don't care) 
        _copy_prefixed_fields(new_starting_section["data"], "start", filtered_stop_data, "enter")
        _extend_section_by_point(new_starting_section, dist_diff, ts_diff)
        logging.debug("after merge, next section is %s" % new_starting_section.data.start_fmt_time)
    else:
        logging.debug("next_section %s is more dense than prev_section %s, merging forwards" % 
            (new_ending_section.data.end_fmt_time, new_starting_section.data.start_fmt_time))
        # merge forward. previous section takes over the stop (e.g. transit -> walking).
        # So the enter should be overwritten with the exit
        _copy_prefixed_fields(filtered_stop_data, "enter", filtered_stop_data, "exit")
        # Then, the previous section's end should be overwritten with this stop's
        # information (since enter now = exit, which set of fields is a don't care) 
        _copy_prefixed_fields(new_ending_section["data"], "end", filtered_stop_data, "exit")
        _extend_section_by_point(new_starting_section, dist_diff, ts_diff)
        logging.debug("after merge, prev section is %s" % new_ending_section.data.end_fmt_time)

    filtered_stop_data["distance"] = 0
    filtered_stop_data["duration"] = 0

def _extend_section_by_point(section, dist_diff, ts_diff):
    section["data"]["distance"] += dist_diff
    section["data"]["duration"] += ts_diff
    section["data"]["distances"].append(dist_diff)
    section["data"]["speeds"].append(dist_diff/ts_diff)

def _copy_prefixed_fields(stop_data, stop_prefix, section_data, section_prefix):
    stop_data[stop_prefix+"_ts"] = section_data[section_prefix+"_ts"]
    stop_data[stop_prefix+"_local_dt"] = section_data[section_prefix+"_local_dt"]
    stop_data[stop_prefix+"_fmt_time"] = section_data[section_prefix+"_fmt_time"]
    stop_data[stop_prefix+"_loc"] = section_data[section_prefix+"_loc"]

def create_and_link_timeline(tl, user_id, trip_map):
    ts = esta.TimeSeries.get_time_series(user_id)
    last_cleaned_place = esdp.get_last_place_entry(esda.CLEANED_PLACE_KEY, user_id)
    cleaned_places = []
    curr_cleaned_start_place = last_cleaned_place
    if curr_cleaned_start_place is None:
        # If it is not present - maybe this user is getting started for the first
        # time, we create an entry based on the first trip from the timeline
        curr_cleaned_start_place = get_filtered_place(tl.first_place())
        logging.debug("no last cleaned place found, created place with id %s" % curr_cleaned_start_place.get_id())
        # We just created this place here, so lets add it to the created places
        # and insert rather than update it
        cleaned_places.append(curr_cleaned_start_place)
    else:
        logging.debug("Cleaned place %s found, using it" % curr_cleaned_start_place.get_id())

    if curr_cleaned_start_place is None:
        # If the timeline has no entries, we give up and return
        return (None, None)

    unsquished_trips = []

    for raw_trip in tl.trips:
        if raw_trip.get_id() in trip_map and not _is_squished_untracked(raw_trip, tl.trips, trip_map):
            # there is a clean representation for this trip, so we can link its
            # start to the curr_cleaned_start_place
            curr_cleaned_trip = trip_map[raw_trip.get_id()]
            raw_start_place = tl.get_object(raw_trip.data.start_place)
            link_trip_start(ts, curr_cleaned_trip, curr_cleaned_start_place, raw_start_place)

            raw_end_place = tl.get_object(raw_trip.data.end_place)
            curr_cleaned_end_place = get_filtered_place(raw_end_place)
            cleaned_places.append(curr_cleaned_end_place)
            link_trip_end(curr_cleaned_trip, curr_cleaned_end_place, raw_end_place)

            curr_cleaned_start_place = curr_cleaned_end_place
            logging.debug("Found mapping %s -> %s, added links" %
                          (raw_trip.get_id(), curr_cleaned_trip.get_id()))
            unsquished_trips.append(curr_cleaned_trip)
        else:
            # this is a squished trip, so we combine the start place with the
            # current start place. We do not need to combine both start and end
            # places, since the end place of one trip is the start place of another.
            # We combine start places instead of end places because when the squishy part ends,
            # we combine the start place of the un-squished trip
            # with the existing cleaned start and create a new entry for the un-squished end
            logging.debug("Found squished trip %s, linking raw start place %s to existing cleaned place %s" %
                          (raw_trip.get_id(), raw_trip.data.start_place, curr_cleaned_start_place.get_id()))
            link_squished_place(curr_cleaned_start_place,
                                tl.get_object(raw_trip.data.start_place))

    logging.debug("Finished creating and linking timeline, returning %d places and %d trips" % (len(cleaned_places), len(list(trip_map.values()))))
    return (last_cleaned_place, esdtl.Timeline(esda.CLEANED_PLACE_KEY,
                                               esda.CLEANED_TRIP_KEY,
                                               cleaned_places,
                                               unsquished_trips))

def link_squished_place(cleaned_place, raw_place):
    cleaned_place_data = cleaned_place.data
    cleaned_place_data.append_raw_place(raw_place.get_id())
    cleaned_place["data"] = cleaned_place_data

def link_trip_start(ts, cleaned_trip, cleaned_start_place, raw_start_place):
    logging.debug("for trip %s start, converting %s to %s" %
                  (cleaned_trip, cleaned_start_place, raw_start_place))
    cleaned_start_place_data = cleaned_start_place.data
    cleaned_trip_data = cleaned_trip.data

    cleaned_trip_data.start_place = cleaned_start_place.get_id()

    # It may be that we are linking to a cleaned place that wasn't generated from
    # this raw place - e.g.
    # trip 1: valid - create cp1 for start and cp2 for end
    # trip 2: squished - link cp2 to start
    # trip 3: valid - link cp2 to start
    # Note that cp2 was originally created from the end place of trip 1 = start place
    # of trip 2. Now it is being linked to the start place of trip 3. While these
    # have to be < 100 m away, they are not guaranteed to be identical.
    # Let's handle this case (https://github.com/e-mission/e-mission-server/issues/385#issuecomment-244793203)
    if (cleaned_start_place_data.location.coordinates !=
                       raw_start_place.data.location.coordinates):
        _fix_squished_place_mismatch(cleaned_trip.user_id, cleaned_trip.get_id(),
                                     ts, cleaned_trip_data, cleaned_start_place_data)

    # We have now reached the end of the squishing. Let's hook up the end information
    cleaned_start_place_data.starting_trip = cleaned_trip.get_id()
    cleaned_start_place_data.exit_ts = cleaned_trip_data.start_ts
    cleaned_start_place_data.exit_fmt_time = cleaned_trip_data.start_fmt_time
    cleaned_start_place_data.exit_local_dt = cleaned_trip_data.start_local_dt

    if cleaned_start_place_data.enter_ts is not None and \
        cleaned_start_place_data.exit_ts is not None:
           cleaned_start_place_data.duration = cleaned_start_place_data.exit_ts - \
                                            cleaned_start_place_data.enter_ts
    else:
           logging.debug("enter_ts = %s, exit_ts = %s, unknown duration" % 
        (cleaned_start_place_data.enter_ts, cleaned_start_place_data.exit_ts))

    # Appended while creating the start place, or while handling squished
    cleaned_start_place_data.append_raw_place(raw_start_place.get_id())

    cleaned_start_place["data"] = cleaned_start_place_data
    cleaned_trip["data"] = cleaned_trip_data

def link_trip_end(cleaned_trip, cleaned_end_place, raw_end_place):
    cleaned_end_place_data = cleaned_end_place.data
    cleaned_trip_data = cleaned_trip.data

    cleaned_trip_data.end_place = cleaned_end_place.get_id()
    cleaned_end_place_data.ending_trip = cleaned_trip.get_id()

    cleaned_end_place["data"] = cleaned_end_place_data
    cleaned_trip["data"] = cleaned_trip_data

def _fix_squished_place_mismatch(user_id, trip_id, ts, cleaned_trip_data, cleaned_start_place_data):
    distance_delta = ecc.calDistance(cleaned_trip_data.start_loc.coordinates,
                                     cleaned_start_place_data.location.coordinates)
    orig_start_ts = cleaned_trip_data.start_ts
    logging.debug("squishing mismatch: resetting trip start_loc %s to cleaned_start_place.location %s" %
                  (cleaned_trip_data.start_loc.coordinates, cleaned_start_place_data.location.coordinates))
    if distance_delta > 100:
        logging.debug("distance_delta = %s > 100, abandoning squish" %
                      (distance_delta))
        return
    else:
        logging.debug("distance_delta = %s < 100, continuing with squish " %
                      (distance_delta))

    # In order to make everything line up, we need to:
    # 1) compute the new trip start ~ 50m at 5km/hr = 36 secs
    # We will approximate to 30 secs to make it consistent with the other locations
    new_ts = cleaned_trip_data.start_ts - 30
    # 2) Reset trip start location and ts
    _overwrite_from_timestamp(cleaned_trip_data, "start",
                              new_ts, cleaned_trip_data.start_local_dt.timezone,
                              cleaned_start_place_data.location)
    # 3) Fix other trip stats
    cleaned_trip_data["distance"] = cleaned_trip_data.distance + distance_delta
    cleaned_trip_data["duration"] = cleaned_trip_data.duration + 30

    logging.debug("fix_squished_place: Fixed trip object = %s" % cleaned_trip_data)

    # 4) Reset section
    section_entries = esdtq.get_cleaned_sections_for_trip(user_id, trip_id)
    if len(section_entries) == 0:
        logging.debug("No sections found, must be untracked time, skipping section and location fixes")
        return

    first_section = section_entries[0]
    first_section_data = first_section.data
    _overwrite_from_timestamp(first_section_data, "start",
                              new_ts, cleaned_trip_data.start_local_dt.timezone,
                              cleaned_start_place_data.location)
    logging.debug("fix_squished_place: Fixed section object = %s" % first_section_data)

    # 5) Fix other section stats
    first_section_data["distance"] = first_section_data.distance + distance_delta
    first_section_data["duration"] = first_section_data.duration + 30
    # including the distances array (move everything by the delta and add a 0 entry at the beginning)
    # [0.0, 13.727242885636501, 13.727251206018853, -> [0.0, distance_delta, 13.727242885636501, 13.727251206018853,
    distances_list = first_section_data["distances"]
    distances_list.insert(1, distance_delta)
    # and the speed array (insert an entry for this first 30 secs)
    # [0.0, 0.45757476285455007, 0.4575750402006284, -> [0.0, distance_delta/30, 0.45757476285455007, 0.4575750402006284,
    speed_list = first_section_data["speeds"]
    logging.debug("fix_squished_place: before inserting, speeds = %s" % speed_list[:10])
    speed_list.insert(1, old_div(float(distance_delta),30))
    logging.debug("fix_squished_place: after inserting, speeds = %s" % speed_list[:10])
    first_section_data["distances"] = distances_list
    first_section_data["speeds"] = speed_list

    first_section["data"] = first_section_data
    ts.update(first_section)

    # 4) Add a new ReconstructedLocation to match the new start point
    if cleaned_start_place_data.enter_ts is not None:
        orig_location = ts.get_entry_at_ts("background/filtered_location", "data.ts", cleaned_start_place_data.enter_ts)
    else:
        first_raw_place = ecwe.Entry(ts.get_entry_from_id(esda.RAW_PLACE_KEY,
                                                          cleaned_start_place_data["raw_places"][0]))
        orig_location = ts.get_entry_at_ts("background/filtered_location", "data.ts", first_raw_place.data.exit_ts)

    orig_location_data = ecwe.Entry(orig_location).data
    # keep the location, override the time
    orig_location_data["ts"] = new_ts
    orig_location_data["local_dt"] = first_section_data.start_local_dt
    orig_location_data["fmt_time"] = first_section_data.start_fmt_time
    orig_location_data["speed"] = 0
    orig_location_data["distance"] = 0
    loc_row = ecwrl.Recreatedlocation(orig_location_data)
    loc_row.mode = first_section_data.sensed_mode
    loc_row.section = first_section.get_id()
    logging.debug("fix_squished_place: added new reconstructed location %s to match new start point" % loc_row)
    ts.insert_data(user_id, esda.CLEANED_LOCATION_KEY, loc_row)

    # 5) Update previous first location data to have the correct speed and distance
    curr_first_loc_doc = ts.get_entry_at_ts(esda.CLEANED_LOCATION_KEY, "data.ts", orig_start_ts)
    if curr_first_loc_doc is None:
        logging.debug("no %s found for ts %s, early return, skipping overwrite")
        return
    curr_first_loc = ecwe.Entry(curr_first_loc_doc)
    curr_first_loc_data = curr_first_loc.data
    logging.debug("fix_squished_place: before updating, old first location data = %s" % loc_row)
    curr_first_loc_data["distance"] = distance_delta
    curr_first_loc_data["speed"] = old_div(float(distance_delta), 30)
    curr_first_loc["data"] = curr_first_loc_data
    logging.debug("fix_squished_place: after updating, old first location data = %s" % curr_first_loc)
    ts.update(curr_first_loc)

    # Validate the distance and speed calculations. Can remove this after validation
    loc_df = esda.get_data_df(esda.CLEANED_LOCATION_KEY, user_id,
                              esda.get_time_query_for_trip_like(esda.CLEANED_SECTION_KEY, first_section.get_id()))
    logging.debug("fix_squished_place: before recomputing for validation, loc_df = %s" % 
        (loc_df[["_id", "ts", "fmt_time", "latitude", "longitude", "distance", "speed"]]).head())
    related_sections = loc_df.section.unique().tolist()
    if len(related_sections) > 1:
        logging.debug("Found %d sections, need to remove the uncommon ones..." % 
            len(related_sections))
        
        section_counts = [np.count_nonzero(loc_df.section == s) for s in related_sections]
        logging.debug("section counts = %s" % list(zip(related_sections, section_counts)))
        # This code should work even if this assert is removed
        # but this is the expectation we have based on the use cases we have seen so far
        assert(min(section_counts) == 1)
       
        # the most common section is the one at the same index as the max
        # count. This is the valid section
        valid_section = related_sections[np.argmax(section_counts)]
        logging.debug("valid_section = %s" % valid_section)

        index_for_invalid_sections = loc_df[loc_df.section != valid_section].index
        logging.debug("index_for_invalid_sections = %s" % index_for_invalid_sections)

        logging.debug("Before dropping, with_speeds_df.tail = %s" % 
            (loc_df[["_id", "section", "ts", "fmt_time", "latitude", "longitude", "speed"]]).tail())

        loc_df.drop(index_for_invalid_sections, inplace=True)
        # If we don't this, the dropped entry is still present with all
        # entries = NaN, the result after recomputing is 
        # 30  5a40a632f6858f5e3b27307b  1.452972e+09  2016-01-16T11:17:28.072312-08:00
        loc_df.reset_index(drop=True, inplace=True)
        logging.debug("After dropping, with_speeds_df.tail = %s" % 
            (loc_df[["_id", "section", "ts", "fmt_time", "latitude", "longitude", "speed"]]).tail())

        # validate that we only have valid sections now
        logging.debug("About to validate that we have only one valid section %s" % 
            loc_df.section.unique())
        assert(len(loc_df.section.unique().tolist()) == 1)

    loc_df.rename(columns={"speed": "from_points_speed", "distance": "from_points_distance"}, inplace=True)
    with_speeds_df = eaicl.add_dist_heading_speed(loc_df)
    logging.debug("fix_squished_place: after recomputing for validation, with_speeds_df.head = %s" % 
        (with_speeds_df[["_id", "ts", "fmt_time", "latitude", "longitude", "distance", "speed", "from_points_speed"]]).head())

    logging.debug("fix_squished_place: after recomputing for validation, with_speeds_df.tail = %s" % 
        (with_speeds_df[["_id", "ts", "fmt_time", "latitude", "longitude", "distance", "speed", "from_points_speed"]]).tail())


    if not ecc.compare_rounded_arrays(with_speeds_df.speed.tolist(), first_section_data["speeds"], 10):
        logging.error("check start: %s != %s" % (with_speeds_df.speed.tolist()[:10], first_section_data["speeds"][:10]))
        logging.error("check end: %s != %s" % (with_speeds_df.speed.tolist()[-10:], first_section_data["speeds"][-10:]))
        if eac.get_config()["intake.cleaning.clean_and_resample.speedDistanceAssertions"]:
            assert False

    if not ecc.compare_rounded_arrays(with_speeds_df.distance.tolist(), first_section_data["distances"], 10):
        logging.error("check start: %s != %s" % (with_speeds_df.distance.tolist()[:10], first_section_data["distances"][:10]))
        logging.error("check end: %s != %s" % (with_speeds_df.speed.tolist()[-10:], first_section_data["speeds"][-10:]))
        if eac.get_config()["intake.cleaning.clean_and_resample.speedDistanceAssertions"]:
            assert False

    if not ecc.compare_rounded_arrays(with_speeds_df.speed.tolist(), with_speeds_df.from_points_speed.tolist(), 10):
        logging.error("check start: %s != %s" % (with_speeds_df.speed.tolist()[:10], with_speeds_df.from_points_speed.tolist()[:10]))
        logging.error("check end: %s != %s" % (with_speeds_df.speed.tolist()[-10:], with_speeds_df.from_points_speed.tolist()[-10:]))
        if eac.get_config()["intake.cleaning.clean_and_resample.speedDistanceAssertions"]:
            assert False

    if not ecc.compare_rounded_arrays(with_speeds_df.distance.tolist(), with_speeds_df.from_points_distance.tolist(), 10):
        logging.error("check start: %s != %s" % (with_speeds_df.distance.tolist()[:10], with_speeds_df.from_points_distance.tolist()[:10]))
        logging.error("check end: %s != %s" % (with_speeds_df.speed.tolist()[-10:], with_speeds_df.from_points_speed.tolist()[-10:]))
        if eac.get_config()["intake.cleaning.clean_and_resample.speedDistanceAssertions"]:
            assert False

def _is_squished_untracked(raw_trip, raw_trip_list, trip_map):
    if raw_trip.metadata.key != esda.RAW_UNTRACKED_KEY:
        logging.debug("_is_squished_untracked: %s is a %s, not %s, returning False" %
                      (raw_trip.get_id(), raw_trip.metadata.key, esda.RAW_UNTRACKED_KEY))
        return False

    cleaned_untracked = trip_map[raw_trip.get_id()]

    index_list = [rt.get_id() for rt in raw_trip_list]
    curr_index = index_list.index(raw_trip.get_id())
    squished_list = [idx in trip_map for idx in index_list]
    try:
        next_unsquished_index = squished_list.index(True, curr_index+1)
    except ValueError as e:
        logging.debug("no unsquished trips found after %s in %s, returning True" %
                      (squished_list, curr_index))
        return True

    next_unsquished_trip = trip_map[raw_trip_list[next_unsquished_index].get_id()]
    logging.debug("curr_index = %s, next unsquished trip = %s at index %s" %
                  (curr_index, next_unsquished_trip.get_id(), next_unsquished_index))

    next_distance = ecc.calDistance(cleaned_untracked.data.start_loc.coordinates,
                       next_unsquished_trip.data.start_loc.coordinates)
    logging.debug("_is_squished_untracked: distance to next clean start (%s) = %s" %
                  (next_unsquished_trip.get_id(), next_distance))

    # 100 is a nice compromise, and what we use elsewhere, notably while determining
    # what is a short trip and should be squished
    if next_distance > 100:
        logging.debug("_is_squished_untracked: distance to next clean start (%s) = %s >= 100, returning False" %
                      (next_unsquished_trip.get_id(), next_distance))
        return False

    logging.debug("_is_squished_untracked: distance to next clean start (%s) = %s < 100, returning True" %
                  (next_unsquished_trip.get_id(), next_distance))
    return True

def format_result(rev_geo_result):
    get_fine = lambda rgr: get_with_fallbacks(rgr["address"], ["road", "neighbourhood"])
    get_coarse = lambda rgr: get_with_fallbacks(rgr["address"], ["city", "town", "county"])
    name = "%s, %s" % (get_fine(rev_geo_result), get_coarse(rev_geo_result))
    return name

def get_with_fallbacks(json, fallback_key_list):
    for key in fallback_key_list:
        if key in json:
            return json[key]
    return ""
