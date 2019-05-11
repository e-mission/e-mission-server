from __future__ import unicode_literals
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import
from future import standard_library
standard_library.install_aliases()
from builtins import *
import logging
import pymongo

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

def get_user_input_for_trip_object(ts, trip_obj, user_input_key):
    tq = estt.TimeQuery("data.start_ts", trip_obj.data.start_ts, trip_obj.data.end_ts)
    # In general, all candiates will have the same start_ts, so no point in
    # sorting by it. Only exception to general rule is when user first provides
    # input before the pipeline is run, and then overwrites after pipeline is
    # run
    potential_candidates = ts.get_data_df(user_input_key, tq)
    if len(potential_candidates) == 0:
        return None

    sorted_pc = potential_candidates.sort_values(by="metadata_write_ts")
    most_recent_entry_id = potential_candidates._id.iloc[-1]
    logging.debug("most recent entry has id %s" % most_recent_entry_id)
    ret_val = ts.get_entry_from_id(user_input_key, most_recent_entry_id)
    logging.debug("and is mapped to entry %s" % ret_val)
    return ret_val

# This is almost an exact copy of get_user_input_for_trip_object, but it
# retrieves an interable instead of a dataframe. So almost everything is
# different and it is hard to unify the implementations. Switching the existing
# function from get_data_df to find_entries may help us unify in the future

def get_user_input_from_cache_series(user_id, trip_obj, user_input_key):
    tq = estt.TimeQuery("data.start_ts", trip_obj.data.start_ts, trip_obj.data.end_ts)
    potential_candidates = estsc.find_entries(user_id, [user_input_key], tq)
    if len(potential_candidates) == 0:
        return None
    sorted_pc = sorted(potential_candidates, key=lambda c:c["metadata"]["write_ts"])
    most_recent_entry = potential_candidates[-1]
    logging.debug("most recent entry has id %s" % most_recent_entry["_id"])
    logging.debug("and is mapped to entry %s" % most_recent_entry)
    return ecwe.Entry(most_recent_entry)
