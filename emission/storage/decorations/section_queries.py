import logging

import emission.core.get_database as edb
import emission.core.wrapper.section as ecws

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

def get_sections(user_id, time_query):
    curr_query = _get_ts_query(time_query)
    curr_query.update({"user_id": user_id})
    return _get_sections_for_query(curr_query)

def get_sections_for_trip(user_id, trip_id):
    curr_query = {"user_id": user_id, "trip_id": trip_id}
    return _get_sections_for_query(curr_query)

def get_sections_for_trip_list(user_id, trip_list):
    curr_query = {"user_id": user_id, "trip_id": {"$in": trip_list}}
    return _get_sections_for_query(curr_query)

def _get_sections_for_query(section_query):
    logging.debug("Returning sections for query %s" % section_query)
    section_doc_cursor = edb.get_section_new_db().find(section_query)
    # TODO: Fix "TripIterator" and return it instead of this list
    return [ecws.Section(doc) for doc in section_doc_cursor]
