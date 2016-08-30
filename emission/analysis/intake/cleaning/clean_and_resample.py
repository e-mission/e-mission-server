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
import logging
import numpy as np
import scipy.interpolate as spi
import pandas as pd
import arrow
import geojson as gj

# Our imports
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

import emission.core.common as ecc

import emission.core.our_geocoder as eco

import attrdict as ad


filtered_trip_excluded = ["start_place", "end_place",
                          "start_ts", "start_fmt_time", "start_local_dt", "start_loc",
                          "_id"]
# We are not copying over any of the exit information from the raw place because
# we may want to squish a bunch of places together, and then the end information
# will come from the final squished place
filtered_place_excluded = ["exit_ts", "exit_local_dt", "exit_fmt_time",
                           "starting_trip", "ending_trip", "duration", "_id"]
filtered_section_excluded = ["trip_id", "start_stop", "end_stop", "distance", "duration",
                             "start_ts", "start_fmt_time", "start_local_dt","start_loc",
                             "_id"]
filtered_stop_excluded = ["trip_id", "ending_section", "starting_section", "_id"]
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
    return save_cleaned_segments_for_timeline(user_id, tl)

def save_cleaned_segments_for_timeline(user_id, tl):
    ts = esta.TimeSeries.get_time_series(user_id)
    trip_map = {}
    id_or_none = lambda wrapper: wrapper.get_id() if wrapper is not None else None
    for trip in tl.trips:
        try:
            filtered_trip = get_filtered_trip(ts, trip)
            logging.debug("For raw trip %s, found filtered trip %s" %
                          (id_or_none(trip), id_or_none(filtered_trip)))
            if filtered_trip is not None:
                trip_map[trip.get_id()] = filtered_trip
        except KeyError, e:
            # We ran into key errors while dealing with mixed filter trip_entries.
            # I think those should be resolved for now, so we can raise the error again
            # But if this is preventing us from making progress, we can comment out the raise
            logging.exception("Found key error %s while processing trip %s" % (e, trip))
            # raise e
        except Exception, e:
            logging.exception("Found error %s while processing trip %s" % (e, trip))
            raise e

    (last_cleaned_place, filtered_tl) = create_and_link_timeline(tl, user_id, trip_map)

    # We have updated the first place entry in the filtered_tl, but everything
    # else is new and needs to be inserted
    if last_cleaned_place is not None:
        logging.debug("last cleaned_place %s was already in database, updating..." % 
            last_cleaned_place)
        ts.update(last_cleaned_place)
    if filtered_tl is not None:
        for entry in filtered_tl:
            ts.insert(entry)

    return tl.last_place()

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
    for section in trip_tl.trips:
        section_map[section.get_id()] = get_filtered_section(filtered_trip_entry, section)

    stop_map = {}
    for stop in trip_tl.places:
        stop_map[stop.get_id()] = get_filtered_stop(filtered_trip_entry, stop)

    # Set the start point and time for the trip from the start point and time
    # from the first section
    first_cleaned_section = section_map[trip_tl.trips[0].get_id()]
    logging.debug("Copying start_ts %s, start_fmt_time %s, start_local_dt from %s to %s" %
                  (first_cleaned_section.data.start_ts, first_cleaned_section.data.start_fmt_time,
                   first_cleaned_section.get_id(), filtered_trip_data))
    _set_extrapolated_start_for_trip(filtered_trip_data, first_cleaned_section)

    trip_distance = sum([section.data.distance for section in section_map.values()]) + \
        sum([stop.data.distance for stop in stop_map.values()])
    filtered_trip_data.distance = trip_distance
    filtered_trip_entry["data"] = filtered_trip_data
    if filtered_trip_data.distance < 100:
        logging.info("Skipped single point trip %s (%s -> %s) of length %s" %
                     (trip.get_id(), trip.data.start_fmt_time,
                      trip.data.end_fmt_time, filtered_trip_data.distance))
        return None

    # After we have linked everything back together. NOW we can save the entries
    linked_tl = link_trip_timeline(trip_tl, section_map, stop_map)
    for entry in linked_tl:
        ts.insert(entry)

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
    except:
        logging.exception("Unable to pre-fill reverse geocoded information, client has to do it")

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
    filtered_section_data.speeds = speeds
    filtered_section_data.distances = distances
    filtered_section_data.distance = sum(distances)
    filtered_section_entry = ecwe.Entry.create_entry(section.user_id,
                                    esda.CLEANED_SECTION_KEY,
                                    filtered_section_data, create_id=True)

    ts = esta.TimeSeries.get_time_series(section.user_id)
    for row in with_speeds_df.to_dict('records'):
        loc_row = ecwrl.Recreatedlocation(row)
        loc_row.mode = section.data.sensed_mode
        loc_row.section = filtered_section_entry.get_id()
        ts.insert_data(section.user_id, esda.CLEANED_LOCATION_KEY, loc_row)

    return filtered_section_entry

def get_filtered_stop(new_trip_entry, stop):
    """
    If we have filtered sections, I guess we need to have filtered stops as well.
    These should also point to the new trip

    :param new_trip_trip: the trip that this stop is part of
    :param stop: the section that this is part of
    :return: None
    """
    filtered_stop_data = ecwst.Stop()
    filtered_stop_data['_id'] = new_trip_entry.get_id()
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

    if section.data.start_stop is None:
        logging.debug("Found first section, need to extrapolate start point")
        raw_trip = ts.get_entry_from_id(esda.RAW_TRIP_KEY, section.data.trip_id)
        raw_start_place = ts.get_entry_from_id(esda.RAW_PLACE_KEY, raw_trip.data.start_place)
        filtered_loc_df = _add_start_point(filtered_loc_df, raw_start_place, ts)

    # Can move this up to get_filtered_section if we don't want ensure that we
    # don't touch filtered_section_data in here
    logging.debug("Setting section start values of %s to first point %s" %
                  (filtered_section_data, filtered_loc_df.iloc[0]))
    _set_extrapolated_start_for_section(filtered_section_data,
                                        filtered_loc_df.iloc[0])

    # if section.data.start_stop is None:
    #     first_point = filtered_loc_list.pop(0)
    #     logging.debug("first section - removing %s to avoid badly interpolated results" % first_point)

    logging.debug("Resampling data of size %s" % len(filtered_loc_df))
    # filtered_loc_list has removed the outliers. Now, we resample the data at
    # 30 sec intervals
    resampled_loc_df = resample(filtered_loc_df, interval=30)

    with_speeds_df = eaicl.add_dist_heading_speed(resampled_loc_df)
    with_speeds_df["idx"] = np.arange(0, len(with_speeds_df))
    with_speeds_df_nona = with_speeds_df.dropna()
    logging.info("removed %d entries containing n/a" % 
        (len(with_speeds_df_nona) - len(with_speeds_df)))
    return with_speeds_df_nona

def _copy_non_excluded(old_data, new_data, excluded_list):
    for key in old_data:
        if key not in excluded_list:
            new_data[key] = old_data[key]

def _add_start_point(filtered_loc_df, raw_start_place, ts):
    raw_start_place_enter_loc_entry = _get_raw_start_place_enter_loc_entry(ts, raw_start_place)
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
        del_time = add_dist / with_speeds_df.speed.median()
    else:
        logging.info("speeds for this section are %s, median is %s, skipping" %
                     (with_speeds_df.speed, with_speeds_df.speed.median()))
        del_time = 0

    new_start_ts = curr_first_point.ts - del_time
    prev_enter_ts = raw_start_place.data.enter_ts if "enter_ts" in raw_start_place.data else None
    if prev_enter_ts is not None and new_start_ts < raw_start_place.data.enter_ts:
        logging.debug("Much extrapolation! new_start_ts %s < prev_enter_ts %s" %
                      (new_start_ts, prev_enter_ts))
        new_start_ts = raw_start_place.data.enter_ts + 3 * 60
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
    return _prepend_new_entry(filtered_loc_df, new_first_point)

def _get_raw_start_place_enter_loc_entry(ts, raw_start_place):
    if raw_start_place.data.enter_ts is not None:
        raw_start_place_enter_loc_entry = ecwe.Entry(
            ts.get_entry_at_ts("background/filtered_location", "data.ts",
                               raw_start_place.data.enter_ts))
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
            "loc": raw_start_place.data.location,
            "latitude": raw_start_place.data.location.coordinates[1],
            "longitude": raw_start_place.data.location.coordinates[0],
            "ts": raw_start_place.data.exit_ts,
            "fmt_time": raw_start_place.data.exit_fmt_time,
            "local_dt": raw_start_place.data.exit_local_dt
        }
        raw_start_place_enter_loc_entry = ecwe.Entry.create_entry(raw_start_place.user_id,
                                                                  "background/filtered_location",
                                                                  dummy_section_start_loc_doc)
    logging.debug("Start place is %s and corresponding location is %s" %
                  (raw_start_place.get_id(), raw_start_place_enter_loc_entry.get_id()))
    return raw_start_place_enter_loc_entry

def _prepend_new_entry(loc_df, entry):
    import emission.storage.timeseries.builtin_timeseries as estb

    new_first_point_row = estb.BuiltinTimeSeries._to_df_entry(entry)
    del new_first_point_row["_id"]
    missing_cols = set(loc_df.columns) - set(new_first_point_row.keys())
    logging.debug("Missing cols = %s" % missing_cols)
    for col in missing_cols:
        new_first_point_row[col] = 0

    still_missing_cols = set(loc_df.columns) - set(new_first_point_row.keys())
    logging.debug("Missing cols = %s" % still_missing_cols)

    still_missing_cols = set(new_first_point_row.keys()) - set(loc_df.columns)
    logging.debug("Missing cols switched around = %s" % still_missing_cols)

    loc_df.loc[-1] = new_first_point_row
    appended_loc_df = loc_df.sort_index().reset_index(drop=True)
    return appended_loc_df

def _set_extrapolated_start_for_section(filtered_section_data, fixed_first_loc):
    filtered_section_data.start_ts = fixed_first_loc.ts
    filtered_section_data.start_local_dt = esdl.get_local_date(fixed_first_loc.ts,
                                                               fixed_first_loc.local_dt_timezone)
    filtered_section_data.start_fmt_time = fixed_first_loc.fmt_time
    filtered_section_data.start_loc = fixed_first_loc["loc"]
    filtered_section_data.duration = filtered_section_data.end_ts - filtered_section_data.start_ts

def _set_extrapolated_start_for_trip(filtered_trip_data, first_section):
    filtered_trip_data.start_ts = first_section.data.start_ts
    filtered_trip_data.start_fmt_time = first_section.data.start_fmt_time
    filtered_trip_data.start_local_dt = first_section.data.start_local_dt
    filtered_trip_data.start_loc = first_section.data.start_loc

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

def resample(filtered_loc_list, interval):
    """
    TODO: There is a problem with working on this on a section by section basis.
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
    :param filtered_loc_list:
    :param interval:
    :return:
    """
    loc_df = pd.DataFrame(filtered_loc_list)
    # See https://github.com/e-mission/e-mission-server/issues/268 for log traces
    #
    # basically, on iOS, due to insufficient smoothing, it is possible for us to
    # have very small segments. Some of these contain zero points, and we skip them
    # in the segmentation stage. Some of them contain one point, and we don't.
    # Ideally, we would strip these sections too and merge the stops on the two sides
    # But that is going to take more time and effort than I have here.
    #
    # So let's just return the one point without resampling in that case, and move on for now
    if len(loc_df) == 0 or len(loc_df) == 1:
        return loc_df
    logging.debug("Resampling entry list %s of size %s" % (loc_df.head(), len(filtered_loc_list)))
    start_ts = loc_df.ts.iloc[0]
    end_ts = loc_df.ts.iloc[-1]
    tz_ranges_df = _get_tz_ranges(loc_df)
    logging.debug("tz_ranges_df = %s" % tz_ranges_df)

    lat_fn = spi.interp1d(x=loc_df.ts, y=loc_df.latitude, bounds_error=False,
                          fill_value='extrapolate')

    lng_fn = spi.interp1d(x=loc_df.ts, y=loc_df.longitude, bounds_error=False,
                          fill_value='extrapolate')

    altitude_fn = spi.interp1d(x=loc_df.ts, y=loc_df.altitude, bounds_error=False,
                          fill_value='extrapolate')

    ts_new = np.append(np.arange(start_ts, end_ts, 30), [end_ts])
    logging.debug("After resampling, using %d points" % ts_new.size)
    lat_new = lat_fn(ts_new)
    lng_new = lng_fn(ts_new)
    alt_new = altitude_fn(ts_new)
    tz_new = [_get_timezone(ts, tz_ranges_df) for ts in ts_new]
    ld_new = [esdl.get_local_date(ts, tz) for (ts, tz) in zip(ts_new, tz_new)]
    loc_new = [gj.Point((lng, lat)) for (lng, lat) in zip(lng_new, lat_new)]
    fmt_time_new = [arrow.get(ts).to(tz).isoformat() for
                        (ts, tz) in zip(ts_new, tz_new)]
    loc_df_new = pd.DataFrame({"latitude": lat_new, "longitude": lng_new,
                               "loc": loc_new, "ts": ts_new, "local_dt": ld_new,
                               "fmt_time": fmt_time_new, "altitude": alt_new})
    return loc_df_new

def _get_timezone(ts, tz_ranges_df):
    # TODO: change this to a dataframe query instead
    sel_entry = tz_ranges_df[(tz_ranges_df.start_ts <= ts) &
                        (tz_ranges_df.end_ts >= ts)]
    if len(sel_entry) != 1:
        logging.warning("len(sel_entry) = %d, using the one with the bigger duration" % len(sel_entry))
        sel_entry["duration"] = sel_entry.end_ts - sel_entry.start_ts
        sel_entry = sel_entry[sel_entry.duration == sel_entry.duration.max()]
    return sel_entry.timezone.iloc[0]

def _get_tz_ranges(loc_df):
    tz_ranges = []
    if len(loc_df) == 0:
        return tz_ranges

    # We know that there is at least one entry, so we can access it with impunity
    curr_start_ts = loc_df.ts.iloc[0]
    curr_tz = loc_df.local_dt_timezone.iloc[0]
    for row in loc_df.to_dict('records'):
        loc_data = ad.AttrDict(row)
        if loc_data.local_dt_timezone != curr_tz:
            tz_ranges.append({'timezone': curr_tz,
                              'start_ts': curr_start_ts,
                              'end_ts': loc_data.ts})
            curr_start_ts = loc_data.ts
            curr_tz = loc_data.local_dt_timezone

    # At the end, always add an entry
    # For cases in which there is only one timezone (common case),
    # this will be the only entry
    tz_ranges.append({'timezone': curr_tz,
                      'start_ts': curr_start_ts,
                      'end_ts': loc_df.ts.iloc[-1]})
    logging.debug("tz_ranges = %s" % tz_ranges)
    return pd.DataFrame(tz_ranges)

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
    stop_data.ending_section = section_map[old_stop.data.ending_section].get_id()
    stop_data.starting_section = section_map[old_stop.data.starting_section].get_id()
    new_stop["data"] = stop_data
    return new_stop

def create_and_link_timeline(tl, user_id, trip_map):
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
        if raw_trip.get_id() in trip_map:
            # there is a clean representation for this trip, so we can link its
            # start to the curr_cleaned_start_place
            curr_cleaned_trip = trip_map[raw_trip.get_id()]
            raw_start_place = tl.get_object(raw_trip.data.start_place)
            link_trip_start(curr_cleaned_trip, curr_cleaned_start_place, raw_start_place)

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
            # current start place we do not need to combine both start and end
            # places, since the end place of one trip is the start place of another. We combine start places instead of end places
            # because when the squishy part ends, we combine the start place of the un-squished trip
            # with the existing cleaned start and create a new entry for the un-squished end
            logging.debug("Found squished trip, linking raw start place %s to new cleaned place %s" %
                          (raw_trip.data.start_place, curr_cleaned_start_place.get_id()))
            link_squished_place(curr_cleaned_start_place,
                                tl.get_object(raw_trip.data.start_place))

    logging.debug("Finished creating and linking timeline, returning %d places and %d trips" % (len(cleaned_places), len(trip_map.values())))
    return (last_cleaned_place, esdtl.Timeline(esda.CLEANED_PLACE_KEY,
                                               esda.CLEANED_TRIP_KEY,
                                               cleaned_places,
                                               unsquished_trips))

def link_squished_place(cleaned_place, raw_place):
    cleaned_place_data = cleaned_place.data
    cleaned_place_data.append_raw_place(raw_place.get_id())
    cleaned_place["data"] = cleaned_place_data

def link_trip_start(cleaned_trip, cleaned_start_place, raw_start_place):
    logging.debug("for trip %s start, converting %s to %s" %
                  (cleaned_trip, cleaned_start_place, raw_start_place))
    cleaned_start_place_data = cleaned_start_place.data
    cleaned_trip_data = cleaned_trip.data

    cleaned_trip_data.start_place = cleaned_start_place.get_id()

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
		(cleaned_start_place_data.enter_ts, cleaned_start_place_data.enter_ts))

    # Appended while creating the start place, or while handling squished
    # TODO: Don't think I need this?
    # cleaned_start_place_data.append_raw_place(raw_trip.data.start_place)

    cleaned_start_place["data"] = cleaned_start_place_data
    cleaned_trip["data"] = cleaned_trip_data

def link_trip_end(cleaned_trip, cleaned_end_place, raw_end_place):
    cleaned_end_place_data = cleaned_end_place.data
    cleaned_trip_data = cleaned_trip.data

    cleaned_trip_data.end_place = cleaned_end_place.get_id()
    cleaned_end_place_data.ending_trip = cleaned_trip.get_id()

    cleaned_end_place["data"] = cleaned_end_place_data
    cleaned_trip["data"] = cleaned_trip_data

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
