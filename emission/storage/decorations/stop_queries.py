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
    user_id = stop_query["user_id"]
    logging.debug("Returning stops for query %s" % stop_query)
    stop_query.update({"metadata.key": "segmentation/raw_stop"})
    logging.debug("updated query = %s" % stop_query)
    
    # Replace direct database calls with TimeSeries abstraction
    ts = esta.TimeSeries.get_time_series(user_id)
    # Don't include user_id in extra_query since it's already in the user_query
    extra_query = {k: v for k, v in stop_query.items() 
                  if k != "metadata.key" and k != "user_id"}
    
    # Use metadata.write_ts for TimeQuery since all entries (including test data) have this field
    # This ensures we get all entries while still leveraging MongoDB sorting
    time_query = estt.TimeQuery("metadata.write_ts", 0, 9999999999)
    stop_docs = ts.find_entries(["segmentation/raw_stop"], 
                               time_query=time_query,
                               extra_query_list=[extra_query])
    
    stop_entries = [ecwe.Entry(doc) for doc in stop_docs]
    
    logging.debug("result count = %d" % len(stop_entries))
    return stop_entries
