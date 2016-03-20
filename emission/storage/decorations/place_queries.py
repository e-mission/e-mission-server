import logging
import pymongo

import emission.core.get_database as edb
import emission.core.wrapper.place as ecwp

def get_last_place(user_id):
    """
    There are many ways to find the last place.  One would be to find the one
    with the max enter_ts.  But that is not performant because we would need to
    retrieve all the enter_ts and find their max, which is expensive. Instead, we
    use the property that we process data in chunks of trips, so the last place
    would have been created and entered but not exited.
    """
    ret_place_doc = edb.get_place_db().find_one({'user_id': user_id,
                                                 'exit_ts' : {'$exists': False}})
    logging.debug("last place doc = %s" % ret_place_doc)
    if ret_place_doc is None:
        return None
    ret_place = ecwp.Place(ret_place_doc)
    assert('exit_ts' not in ret_place)
    assert('exit_fmt_time' not in ret_place)
    assert('starting_trip' not in ret_place)
    return ret_place

def _get_ts_query(tq):
    time_key = tq.timeType
    ret_query = {time_key : {"$lt": tq.endTs}}
    if (tq.startTs is not None):
        ret_query[time_key].update({"$gte": tq.startTs})
    return ret_query

def get_place(place_id):
    return ecwp.Place(edb.get_place_db().find_one({"_id": place_id}))

def get_places(user_id, time_query):
    curr_query = _get_ts_query(time_query)
    curr_query.update({"user_id": user_id})
    place_doc_cursor = edb.get_place_db().find(curr_query).sort(time_query.timeType, pymongo.ASCENDING)
    logging.debug("%d places found in database" % place_doc_cursor.count())
    # TODO: Fix "TripIterator" and return it instead of this list
    return [ecwp.Place(doc) for doc in place_doc_cursor]

def get_aggregate_places(time_query):
    curr_query = _get_ts_query(time_query)
    place_doc_cursor = edb.get_place_db().find(curr_query).sort(time_query.timeType, pymongo.ASCENDING)
    print ("%d places found in database" % place_doc_cursor.count())
    return [ecwp.Place(doc) for doc in place_doc_cursor]

def get_aggregate_places_in_box(time_query, box):
    curr_query = _get_ts_query(time_query)
    curr_query.update({"loc": {"$geoWithin" : {"$box" :box}}})
    place_doc_cursor = edb.get_place_db().find(curr_query).sort(time_query.timeType, pymongo.ASCENDING)
    return [ecwp.Place(doc) for doc in place_doc_cursor]

def create_new_place(user_id):
    _id = edb.get_place_db().save({'user_id': user_id})
    logging.debug("Created new place %s for user %s" % (_id, user_id))
    return ecwp.Place({"_id": _id, 'user_id': user_id})

def save_place(place):
    edb.get_place_db().save(place)
