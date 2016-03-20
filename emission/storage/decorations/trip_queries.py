import logging
import pymongo

import emission.net.usercache.abstract_usercache as enua

import emission.core.get_database as edb
import emission.core.wrapper.trip as ecwt
import emission.core.wrapper.section as ecws
import emission.core.wrapper.stop as ecwst

import emission.storage.decorations.timeline as esdt

def create_new_trip(user_id):
    _id = edb.get_trip_new_db().save({"user_id": user_id})
    return ecwt.Trip({"_id": _id, "user_id": user_id})


def save_trip(trip):
    edb.get_trip_new_db().save(trip)


def _get_ts_query(tq):
    time_key = tq.timeType
    ret_query = {time_key: {"$lt": tq.endTs}}
    if (tq.startTs is not None):
        ret_query[time_key].update({"$gte": tq.startTs})
    return ret_query


def get_trips(user_id, time_query):
    curr_query = _get_ts_query(time_query)
    curr_query.update({"user_id": user_id})
    trip_doc_cursor = edb.get_trip_new_db().find(curr_query).sort(time_query.timeType, pymongo.ASCENDING)
    # TODO: Fix "TripIterator" and return it instead of this list
    return [ecwt.Trip(doc) for doc in trip_doc_cursor]

def get_aggregate_trips(time_query):
    curr_query = _get_ts_query(time_query)
    trip_doc_cursor = edb.get_trip_new_db().find(curr_query).sort(time_query.timeType, pymongo.ASCENDING)
    print "trip_doc_cursor.count() is %d" % trip_doc_cursor.count()
    return [ecwt.Trip(doc) for doc in trip_doc_cursor]

def get_aggregate_trips_box(time_query, box):
    curr_query = _get_ts_query(time_query)
    curr_query.update({"loc" : {"$geoWithin" : {"$box": box}}})
    trip_doc_cursor = edb.get_trip_new_db().find(curr_query).sort(time_query.timeType, pymongo.ASCENDING)
    return [ecwt.Trip(doc) for doc in trip_doc_cursor]

def get_trip(trip_id):
    """
    Returns the trip for specified trip id.
    :rtype : emission.core.wrapper.Trip
    """
    return ecwt.Trip(edb.get_trip_new_db().find_one({"_id": trip_id}))


def get_time_query_for_trip(trip_id):
    trip = get_trip(trip_id)
    return enua.UserCache.TimeQuery("write_ts", trip.start_ts, trip.end_ts)


def get_sections_for_trip(user_id, trip_id):
    """
    Get the set of sections that are children of this trip.
    """
    section_doc_cursor = edb.get_section_new_db().find({"user_id": user_id, "trip_id": trip_id}).sort("start_ts", pymongo.ASCENDING)
    return [ecws.Section(doc) for doc in section_doc_cursor]


def get_stops_for_trip(user_id, trip_id):
    """
    Get the set of sections that are children of this trip.
    """
    stop_doc_cursor = edb.get_stop_db().find({"user_id": user_id, "trip_id": trip_id}).sort("enter_ts", pymongo.ASCENDING)
    logging.debug("About to execute query %s" % {"user_id": user_id, "trip_id": trip_id})
    return [ecwst.Stop(doc) for doc in stop_doc_cursor]


def get_timeline_for_trip(user_id, trip_id):
    """
    Get an ordered sequence of sections and stops corresponding to this trip.
    """
    return esdt.Timeline(get_stops_for_trip(user_id, trip_id),
                         get_sections_for_trip(user_id, trip_id))

