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

