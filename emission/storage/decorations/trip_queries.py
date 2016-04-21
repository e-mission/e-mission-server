import logging
import pymongo

import emission.storage.timeseries.timequery as estt

import emission.core.get_database as edb
import emission.core.wrapper.rawtrip as ecwrt
import emission.core.wrapper.entry as ecwe

import emission.storage.timeseries.abstract_timeseries as esta
import emission.storage.decorations.timeline as esdt

def get_sections_for_trip(user_id, trip_id):
    # type: (UUID, object_id) -> list(sections)
    """
    Get the set of sections that are children of this trip.
    """
    query = {"user_id": user_id, "data.trip_id": trip_id,
             "metadata.key": "segmentation/raw_section"}
    section_doc_cursor = edb.get_analysis_timeseries_db().find(query).sort(
        "data.start_ts", pymongo.ASCENDING)
    logging.debug("Results for section query %s = %s" % (query, list(section_doc_cursor)))
    section_doc_cursor = edb.get_analysis_timeseries_db().find(query).sort(
        "data.start_ts", pymongo.ASCENDING)
    return [ecwe.Entry(doc) for doc in section_doc_cursor]


def get_stops_for_trip(user_id, trip_id):
    """
    Get the set of sections that are children of this trip.
    """
    query = {"user_id": user_id, "data.trip_id": trip_id,
             "metadata.key": "segmentation/raw_stop"}
    stop_doc_cursor = edb.get_analysis_timeseries_db().find(query).sort(
        "data.enter_ts", pymongo.ASCENDING)
    logging.debug("About to execute query %s" % query)
    logging.debug(
        "Results for stop query %s = %s" % (query, list(stop_doc_cursor)))
    stop_doc_cursor = edb.get_analysis_timeseries_db().find(query).sort(
        "data.enter_ts", pymongo.ASCENDING)
    return [ecwe.Entry(doc) for doc in stop_doc_cursor]


def get_timeline_for_trip(user_id, trip_id):
    """
    Get an ordered sequence of sections and stops corresponding to this trip.
    """
    return esdt.Timeline(get_stops_for_trip(user_id, trip_id),
                         get_sections_for_trip(user_id, trip_id))

