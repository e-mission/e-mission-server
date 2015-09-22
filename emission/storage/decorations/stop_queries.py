import logging
import pymongo

import emission.core.get_database as edb
import emission.core.wrapper.stop as ecws

def _get_ts_query(tq):
    time_key = tq.timeType
    ret_query = {time_key : {"$lt": tq.endTs}}
    if (tq.startTs is not None):
        ret_query[time_key].update({"$gte": tq.startTs})
    return ret_query

def get_stops(user_id, time_query):
    curr_query = _get_ts_query(time_query)
    curr_query.update({"user_id": user_id})
    return _get_stops_for_query(curr_query, time_query.timeType)

def create_new_stop(user_id, trip_id):
    _id = edb.get_stop_db().save({'user_id': user_id, "trip_id": trip_id})
    logging.debug("Created new stop %s for user %s" % (_id, user_id))
    return ecws.Stop({"_id": _id, 'user_id': user_id, "trip_id": trip_id})

def save_stop(stop):
    edb.get_stop_db().save(stop)

def get_stops_for_trip(user_id, trip_id):
    curr_query = {"user_id": user_id, "trip_id": trip_id}
    return _get_stops_for_query(curr_query, "enter_ts")

def get_stops_for_trip_list(user_id, trip_list):
    curr_query = {"user_id": user_id, "trip_id": {"$in": trip_list}}
    return _get_stops_for_query(curr_query, "enter_ts")

def _get_stops_for_query(stop_query, sort_key):
    logging.debug("Returning stops for query %s" % stop_query)
    stop_doc_cursor = edb.get_stop_db().find(stop_query).sort(sort_key, pymongo.ASCENDING)
    # TODO: Fix "TripIterator" and return it instead of this list
    return [ecws.Stop(doc) for doc in stop_doc_cursor]
