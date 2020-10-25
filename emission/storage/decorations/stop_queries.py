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
import emission.storage.timeseries.abstract_timeseries as esta

import emission.core.get_database as edb
import emission.core.wrapper.stop as ecws
import emission.core.wrapper.entry as ecwe

def get_stops_for_trip(user_id, trip_id):
    curr_query = {"user_id": user_id, "data.trip_id": trip_id}
    return _get_stops_for_query(curr_query, "data.enter_ts")

def get_stops_for_trip_list(user_id, trip_list):
    curr_query = {"user_id": user_id, "data.trip_id": {"$in": trip_list}}
    return _get_stops_for_query(curr_query, "data.enter_ts")

def _get_stops_for_query(stop_query, sort_key):
    logging.debug("Returning stops for query %s" % stop_query)
    stop_query.update({"metadata.key": "segmentation/raw_stop"})
    logging.debug("updated query = %s" % stop_query)
    stop_doc_cursor = edb.get_analysis_timeseries_db().find(stop_query).sort(
        sort_key, pymongo.ASCENDING)
    logging.debug("result count = %d" % edb.get_analysis_timeseries_db().count_documents(stop_query))
    return [ecwe.Entry(doc) for doc in stop_doc_cursor]
