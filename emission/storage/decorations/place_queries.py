from __future__ import unicode_literals
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import
from future import standard_library
standard_library.install_aliases()
from builtins import *
import logging

import emission.core.get_database as edb
import emission.core.wrapper.entry as ecwe
import emission.storage.timeseries.abstract_timeseries as esta
import emission.storage.decorations.analysis_timeseries_queries as esda


def get_last_place_entry(key, user_id):
    """
    There are many ways to find the last place.  One would be to find the one
    with the max enter_ts.  But that is not performant because we would need to
    retrieve all the enter_ts and find their max, which is expensive. Instead, we
    use the property that we process data in chunks of trips, so the last place
    would have been created and entered but not exited.
    :param key:
    """
    ts = esta.TimeSeries.get_time_series(user_id)
    ret_place_doc = ts.analysis_timeseries_db.find_one({'user_id': user_id,
                                                        'metadata.key': key,
                                                        'data.exit_ts' : {'$exists': False}})
    logging.debug("last place doc = %s" % ret_place_doc)
    if ret_place_doc is None:
        return None
    ret_place = ecwe.Entry(ret_place_doc)
    assert('exit_ts' not in ret_place.data)
    assert('exit_fmt_time' not in ret_place.data)
    assert('starting_trip' not in ret_place.data)
    return ret_place

def get_first_place_entry(key, user_id):
    """
    Similar to get_last_place_entry, only finding one with only an exit_ts
    and no enter_ts.
    """
    ts = esta.TimeSeries.get_time_series(user_id)
    ret_place_doc = ts.analysis_timeseries_db.find_one({'user_id': user_id,
                                                        'metadata.key': key,
                                                        'data.enter_ts' : {'$exists': False}})
    logging.debug("first place doc = %s" % ret_place_doc)
    if ret_place_doc is None:
        return None
    ret_place = ecwe.Entry(ret_place_doc)
    assert('enter_ts' not in ret_place.data)
    assert('enter_fmt_time' not in ret_place.data)
    assert('ending_trip' not in ret_place.data)
    return ret_place

def get_last_place_before(place_key, reset_ts, user_id):
    """
    Unlike `get_last_place_before` which returns the last place in the
    timeline, this returns the last place before a particular timestamp.
    Used to reset the pipeline, for example.

    To implement this, we can't just look for places before that timestamp,
    because then we will get a list. And we don't want to retrieve all of them
    and sort either.

    We can look for places that exit after that timestamp, but that will also
    give a list. But hopefully, a shorter list, so that we don't have to sort
    as much.  I can't think of an alternative that doesn't require sorting.

    Oh wait! There is an alternative!

    We can look for the place that has an enter timestamp before the ts and an
    exit timestamp after, or a trip that has a start timestamp before the ts
    and an end timestamp after. We should only find one. And if we find the
    trip then the place is its start place.

    Note that these correspond to the two use cases in 
    https://github.com/e-mission/e-mission-server/issues/333
    """
    trip_key_query = _get_trip_key_query(place_key)
    logging.debug("Looking for last place before %s" % reset_ts)

    ts = esta.TimeSeries.get_time_series(user_id)
    
    # Replace direct database calls with TimeSeries abstraction
    # Don't include user_id in extra_query since it's already in the user_query
    place_docs = ts.find_entries([place_key])
    user_places = [ecwe.Entry(doc) for doc in place_docs]
    
    logging.debug("all places for this user = %s" % 
                 [{"_id": place.get_id(), 
                   "enter_fmt_time": place.data.get("enter_fmt_time"),
                   "exit_fmt_time": place.data.get("exit_fmt_time")} 
                  for place in user_places])
    
    # Find place that spans the reset_ts
    ret_place_doc = ts.analysis_timeseries_db.find_one({'user_id': user_id,
                                                        'metadata.key': place_key,
                                                        'data.exit_ts' : {'$gt': reset_ts},
                                                        'data.enter_ts': {'$lt': reset_ts}
                                                       })
    logging.debug("last place doc for user %s = %s" % (user_id, ret_place_doc))
    
    # Find trip that spans the reset_ts
    ret_trip_doc = ts.analysis_timeseries_db.find_one({'user_id': user_id,
                                                        'metadata.key': trip_key_query,
                                                        'data.end_ts' : {'$gt': reset_ts},
                                                        'data.start_ts': {'$lt': reset_ts}
                                                       })
    logging.debug("last trip doc for user %s = %s" % (user_id, ret_trip_doc))
    if ret_place_doc is None and ret_trip_doc is None:
        # Check to see if the pipeline ended before this
        last_place = get_last_place_entry(place_key, user_id)
        logging.debug("last_place = %s, reset_ts = %s" % 
            (last_place, reset_ts))
        if last_place is None:
            return None
        elif last_place.data.enter_ts is None:
            return None
        elif last_place.data.enter_ts < reset_ts:
            return last_place
        else:
            raise ValueError("No trip or place straddling time %s for user %s" % 
                (reset_ts, user_id))
    if ret_place_doc is None:
        assert ret_trip_doc is not None
        logging.info("ret_trip_doc start = %s, end = %s" % 
            (ret_trip_doc["data"]["start_fmt_time"],
             ret_trip_doc["data"]["end_fmt_time"]))
        ret_place_doc = esda.get_entry(place_key, ret_trip_doc["data"]['start_place'])

    assert ret_place_doc is not None
    ret_place = ecwe.Entry(ret_place_doc)
    return ret_place

def _get_trip_key_query(place_key):
    if place_key == esda.CONFIRMED_PLACE_KEY:
        return {"$in": [esda.CONFIRMED_TRIP_KEY, esda.CONFIRMED_UNTRACKED_KEY]}
    if place_key == esda.CLEANED_PLACE_KEY:
        return {"$in": [esda.CLEANED_TRIP_KEY, esda.CLEANED_UNTRACKED_KEY]}
    elif place_key == esda.RAW_PLACE_KEY:
        return {"$in": [esda.RAW_TRIP_KEY, esda.RAW_UNTRACKED_KEY]}
    else:
        raise RuntimeException("Invalid place key %s" % place_key)
