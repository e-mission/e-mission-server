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

def get_user_input_for_trip(trip_key, user_id, trip_id, user_input_key):
    ts = esta.TimeSeries.get_time_series(user_id)
    trip_obj = ts.get_entry_from_id(trip_key, trip_id)
    return get_user_input_for_trip_object(ts, trip_obj, user_input_key)

# Additional checks to be consistent with the phone code
# www/js/diary/services.js
# Since that has been tested the most
# If we no longer need these checks (maybe with trip editing), we can remove them
def valid_user_input(trip_obj):
    def curried(user_input):
        # we know that the trip is cleaned so we can use the fmt_time
        # but the confirm objects are not necessarily filled out
        fmt_ts = lambda ts, tz: arrow.get(ts).to(tz)
        logging.debug("Comparing user input %s: %s -> %s, trip %s -> %s, checks are (%s) && (%s) || (%s)" % (
            user_input.data.label,
            fmt_ts(user_input.data.start_ts, user_input.metadata.time_zone),
            fmt_ts(user_input.data.end_ts, user_input.metadata.time_zone),
            trip_obj.data.start_fmt_time, trip_obj.data.end_fmt_time,
            (user_input.data.start_ts >= trip_obj.data.start_ts),
            (user_input.data.end_ts <= trip_obj.data.end_ts),
            ((user_input.data.end_ts - trip_obj.data.end_ts) <= 5 * 60)
        ))
        return (user_input.data.start_ts >= trip_obj.data.start_ts and
            (user_input.data.end_ts <= trip_obj.data.end_ts or
            ((user_input.data.end_ts - trip_obj.data.end_ts) <= 5 * 60)))
    return curried

def final_candidate(trip_obj, potential_candidates):
    potential_candidate_objects = [ecwe.Entry(c) for c in potential_candidates]
    extra_filtered_potential_candidates = list(filter(valid_user_input(trip_obj), potential_candidate_objects))
    if len(extra_filtered_potential_candidates) == 0:
        return None

    # In general, all candiates will have the same start_ts, so no point in
    # sorting by it. Only exception to general rule is when user first provides
    # input before the pipeline is run, and then overwrites after pipeline is
    # run
    sorted_pc = sorted(extra_filtered_potential_candidates, key=lambda c:c["metadata"]["write_ts"])
    logging.debug("sorted candidates are %s" % [(c.metadata.write_fmt_time, c.data.label) for c in sorted_pc])
    most_recent_entry = sorted_pc[-1]
    logging.debug("most recent entry is %s, %s" % 
        (most_recent_entry.metadata.write_fmt_time, most_recent_entry.data.label))
    return most_recent_entry

def get_user_input_for_trip_object(ts, trip_obj, user_input_key):
    tq = estt.TimeQuery("data.start_ts", trip_obj.data.start_ts, trip_obj.data.end_ts)
    potential_candidates = ts.find_entries([user_input_key], tq)
    return final_candidate(trip_obj, potential_candidates)

# This is almost an exact copy of get_user_input_for_trip_object, but it
# retrieves an interable instead of a dataframe. So almost everything is
# different and it is hard to unify the implementations. Switching the existing
# function from get_data_df to find_entries may help us unify in the future

def get_user_input_from_cache_series(user_id, trip_obj, user_input_key):
    tq = estt.TimeQuery("data.start_ts", trip_obj.data.start_ts, trip_obj.data.end_ts)
    potential_candidates = estsc.find_entries(user_id, [user_input_key], tq)
    return final_candidate(trip_obj, potential_candidates)
