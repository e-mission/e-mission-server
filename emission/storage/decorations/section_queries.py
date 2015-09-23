import logging
import pymongo

import emission.core.get_database as edb
import emission.core.wrapper.section as ecws

import emission.net.usercache.abstract_usercache as enua

def create_new_section(user_id, trip_id):
    _id = edb.get_section_new_db().save({"user_id": user_id, "trip_id": trip_id})
    return ecws.Section({"_id": _id, "user_id": user_id, "trip_id": trip_id})

def save_section(section):
    edb.get_section_new_db().save(section)

def _get_ts_query(tq):
    time_key = tq.timeType
    ret_query = {time_key : {"$lt": tq.endTs}}
    if (tq.startTs is not None):
        ret_query[time_key].update({"$gte": tq.startTs})
    return ret_query

def get_section(section_id):
    return ecws.Section(edb.get_section_new_db().find_one({"_id": section_id}))

def get_time_query_for_section(section_id):
    section = get_section(section_id)
    return enua.UserCache.TimeQuery("write_ts", section.start_ts, section.end_ts + 20)

def get_sections(user_id, time_query):
    curr_query = _get_ts_query(time_query)
    curr_query.update({"user_id": user_id})
    return _get_sections_for_query(curr_query, time_query.timeType)

def get_sections_for_trip(user_id, trip_id):
    curr_query = {"user_id": user_id, "trip_id": trip_id}
    return _get_sections_for_query(curr_query, "start_ts")

def get_sections_for_trip_list(user_id, trip_list):
    curr_query = {"user_id": user_id, "trip_id": {"$in": trip_list}}
    return _get_sections_for_query(curr_query, "start_ts")

def _get_sections_for_query(section_query, sort_field):
    logging.debug("Returning sections for query %s" % section_query)
    section_doc_cursor = edb.get_section_new_db().find(section_query).sort(sort_field, pymongo.ASCENDING)
    # TODO: Fix "TripIterator" and return it instead of this list
    return [ecws.Section(doc) for doc in section_doc_cursor]
