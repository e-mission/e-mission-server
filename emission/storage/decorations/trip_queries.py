from __future__ import unicode_literals
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import
from future import standard_library
standard_library.install_aliases()
from builtins import *
import logging
import pymongo
import arrow

import emission.storage.timeseries.timequery as estt

import emission.core.get_database as edb
import emission.core.wrapper.rawtrip as ecwrt
import emission.core.wrapper.entry as ecwe

import emission.storage.timeseries.abstract_timeseries as esta
import emission.storage.timeseries.cache_series as estsc
import emission.storage.decorations.timeline as esdt
import emission.storage.decorations.analysis_timeseries_queries as esda

def get_raw_sections_for_trip(user_id, trip_id):
    return get_sections_for_trip("segmentation/raw_section", user_id, trip_id)

def get_cleaned_sections_for_trip(user_id, trip_id):
    return get_sections_for_trip("analysis/cleaned_section", user_id, trip_id)

def get_raw_stops_for_trip(user_id, trip_id):
    return get_stops_for_trip("segmentation/raw_stop", user_id, trip_id)

def get_cleaned_stops_for_trip(user_id, trip_id):
    return get_stops_for_trip("analysis/cleaned_stop", user_id, trip_id)

def get_raw_timeline_for_trip(user_id, trip_id):
    """
    Get an ordered sequence of sections and stops corresponding to this trip.
    """
    return esdt.Timeline(esda.RAW_STOP_KEY, esda.RAW_SECTION_KEY,
                         get_raw_stops_for_trip(user_id, trip_id),
                         get_raw_sections_for_trip(user_id, trip_id))

def get_cleaned_timeline_for_trip(user_id, trip_id):
    """
    Get an ordered sequence of sections and stops corresponding to this trip.
    """
    return esdt.Timeline(esda.CLEANED_STOP_KEY, esda.CLEANED_SECTION_KEY,
                         get_cleaned_stops_for_trip(user_id, trip_id),
                         get_cleaned_sections_for_trip(user_id, trip_id))

def get_sections_for_trip(key, user_id, trip_id):
    # type: (UUID, object_id) -> list(sections)
    """
    Get the set of sections that are children of this trip.
    """
    query = {"user_id": user_id, "data.trip_id": trip_id,
             "metadata.key": key}
    logging.debug("About to execute query %s with sort_key %s" % (query, "data.start_ts"))
    section_doc_cursor = edb.get_analysis_timeseries_db().find(query).sort(
        "data.start_ts", pymongo.ASCENDING)
    return [ecwe.Entry(doc) for doc in section_doc_cursor]

def get_stops_for_trip(key, user_id, trip_id):
    """
    Get the set of sections that are children of this trip.
    """
    query = {"user_id": user_id, "data.trip_id": trip_id,
             "metadata.key": key}
    logging.debug("About to execute query %s with sort_key %s" % (query, "data.enter_ts"))
    stop_doc_cursor = edb.get_analysis_timeseries_db().find(query).sort(
        "data.enter_ts", pymongo.ASCENDING)
    return [ecwe.Entry(doc) for doc in stop_doc_cursor]

def _get_next_cleaned_trip(ts, trip_obj):
    """
    Find the next trip in the timeline
    """
    next_place = ts.get_entry_from_id(esda.CLEANED_PLACE_KEY, trip_obj.data.end_place)
    if next_place is None:
        return None
    else:
        next_trip = ts.get_entry_from_id(esda.CLEANED_TRIP_KEY, next_place.data.starting_trip)
        return next_trip

def get_user_input_for_trip(trip_key, user_id, trip_id, user_input_key):
    ts = esta.TimeSeries.get_time_series(user_id)
    trip_obj = ts.get_entry_from_id(trip_key, trip_id)
    return get_user_input_for_trip_object(ts, trip_obj, user_input_key)

# Additional checks to be consistent with the phone code
# www/js/diary/services.js
# Since that has been tested the most
# If we no longer need these checks (maybe with trip editing), we can remove them
def valid_user_input_for_trip(ts, trip_obj, user_input):
    # we know that the trip is cleaned so we can use the fmt_time
    # but the confirm objects are not necessarily filled out
    fmt_ts = lambda ts, tz: arrow.get(ts).to(tz)
    logging.debug("Comparing user input %s: %s -> %s, trip %s -> %s, start checks are (%s && %s) and end checks are (%s || %s)" % (
        user_input.data.label,
        fmt_ts(user_input.data.start_ts, user_input.metadata.time_zone),
        fmt_ts(user_input.data.end_ts, user_input.metadata.time_zone),
        trip_obj.data.start_fmt_time, trip_obj.data.end_fmt_time,
        (user_input.data.start_ts >= trip_obj.data.start_ts),
        (user_input.data.start_ts <= trip_obj.data.end_ts),
        (user_input.data.end_ts <= trip_obj.data.end_ts),
        ((user_input.data.end_ts - trip_obj.data.end_ts) <= 15 * 60)
    ))
    start_checks = (user_input.data.start_ts >= trip_obj.data.start_ts and
        user_input.data.start_ts >= trip_obj.data.start_ts)
    end_checks = (user_input.data.end_ts <= trip_obj.data.end_ts or
        ((user_input.data.end_ts - trip_obj.data.end_ts) <= 15 * 60))
    if start_checks and not end_checks:
        logging.debug("Handling corner case where start check matches, but end check does not")
        next_trip_obj = _get_next_cleaned_trip(ts, trip_obj)
        if next_trip_obj is not None:
            end_checks = user_input.data.end_ts <= next_trip_obj.data.start_ts
            logging.debug("Second level of end checks when the next trip is defined (%s <= %s) = %s" % (
                user_input.data.end_ts, next_trip_obj.data.start_ts, end_checks))
        else:
            end_checks = True
            logging.debug("Second level of end checks when the next trip is not defined = %s" % end_checks)
        if end_checks:
            # If we have flipped the values, check to see that there is sufficient overlap
            # https://github.com/e-mission/e-mission-docs/issues/476#issuecomment-747587041
            overlapDuration = min(user_input.data.end_ts, trip_obj.data.end_ts) - max(user_input.data.start_ts, trip_obj.data.start_ts)
            logging.debug("Flipped endCheck, overlap(%s)/trip(%s) = %s" %
                (overlapDuration, trip_obj.data.duration, (overlapDuration / trip_obj.data.duration)));
            end_checks = (overlapDuration/trip_obj.data.duration) > 0.5;
    return start_checks and end_checks

def valid_user_input(ts, trip_obj):
    def curried(user_input):
        return valid_user_input_for_trip(ts, trip_obj, user_input)
    return curried

def final_candidate(filter_fn, potential_candidates):
    potential_candidate_objects = [ecwe.Entry(c) for c in potential_candidates]
    extra_filtered_potential_candidates = list(filter(filter_fn, potential_candidate_objects))
    if len(extra_filtered_potential_candidates) == 0:
        return None

    # In general, all candiates will have the same start_ts, so no point in
    # sorting by it. Only exception to general rule is when user first provides
    # input before the pipeline is run, and then overwrites after pipeline is
    # run
    sorted_pc = sorted(extra_filtered_potential_candidates, key=lambda c:c["metadata"]["write_ts"])
    entry_detail = lambda c: c.data.label if "label" in c.data else c.data.start_fmt_time
    logging.debug("sorted candidates are %s" %
        [{"write_fmt_time": c.metadata.write_fmt_time, "detail": entry_detail(c)} for c in sorted_pc])
    most_recent_entry = sorted_pc[-1]
    logging.debug("most recent entry is %s, %s" %
        (most_recent_entry.metadata.write_fmt_time, entry_detail(most_recent_entry)))
    return most_recent_entry

def get_user_input_for_trip_object(ts, trip_obj, user_input_key):
    tq = estt.TimeQuery("data.start_ts", trip_obj.data.start_ts, trip_obj.data.end_ts)
    potential_candidates = ts.find_entries([user_input_key], tq)
    return final_candidate(valid_user_input(ts, trip_obj), potential_candidates)

# This is almost an exact copy of get_user_input_for_trip_object, but it
# retrieves an interable instead of a dataframe. So almost everything is
# different and it is hard to unify the implementations. Switching the existing
# function from get_data_df to find_entries may help us unify in the future

def get_user_input_from_cache_series(user_id, trip_obj, user_input_key):
    tq = estt.TimeQuery("data.start_ts", trip_obj.data.start_ts, trip_obj.data.end_ts)
    ts = esta.TimeSeries.get_time_series(user_id)
    potential_candidates = estsc.find_entries(user_id, [user_input_key], tq)
    return final_candidate(valid_user_input(ts, trip_obj), potential_candidates)

def valid_trip(ts, user_input):
    def curried(trip_obj):
        return valid_user_input_for_trip(ts, trip_obj, user_input)
    return curried

def get_trip_for_user_input_obj(ts, ui_obj):
    # the match check that we have is:
    # user input can start after trip start
    # user input can end before trip end OR user input is within 5 mins of trip end
    # Given those considerations, there is no principled query for trip data
    # that fits into our query model
    # the trip start is before the user input start, but that can go until eternity
    # and the trip end can be either before or after the user input end
    # we know that the trip end is after the user input start, but again, that
    # can go on until now.
    # As a workaround, let us assume that the trip start is no more than a day
    # before the start of the ui object, which seems like a fairly conservative
    # assumption
    ONE_DAY = 24 * 60 * 60
    tq = estt.TimeQuery("data.start_ts", ui_obj.data.start_ts - ONE_DAY,
        ui_obj.data.start_ts + ONE_DAY)
    potential_candidates = ts.find_entries(["analysis/confirmed_trip"], tq)
    return final_candidate(valid_trip(ts, ui_obj), potential_candidates)
