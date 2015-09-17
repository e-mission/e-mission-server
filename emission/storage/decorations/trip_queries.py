import logging

import emission.core.get_database as edb
import emission.core.wrapper.trip as ecwt

def create_new_trip(user_id):
    _id = edb.get_trip_new_db().save({"user_id": user_id})
    return ecwt.Trip({"_id": _id, "user_id": user_id})

def save_trip(trip):
    edb.get_trip_new_db().save(trip)

def _get_ts_query(tq):
    time_key = tq.timeType
    ret_query = {time_key : {"$lt": tq.endTs}}
    if (tq.startTs is not None):
        ret_query[time_key].update({"$gte": tq.startTs})
    return ret_query

def get_trips(user_id, time_query):
    trip_doc_cursor = edb.get_trip_new_db().find(_get_ts_query(time_query))
    # TODO: Fix "TripIterator" and return it instead of this list
    return [ecwt.Trip(doc) for doc in trip_doc_cursor]
