import logging

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
    trip_key = _get_trip_key(place_key)

    ts = esta.TimeSeries.get_time_series(user_id)
    ret_place_doc = ts.analysis_timeseries_db.find_one({'user_id': user_id,
                                                        'metadata.key': place_key,
                                                        'data.exit_ts' : {'$gt': reset_ts},
                                                        'data.enter_ts': {'$lt': reset_ts}
                                                       })
    logging.debug("last place doc = %s" % ret_place_doc)
    ret_trip_doc = ts.analysis_timeseries_db.find_one({'user_id': user_id,
                                                        'metadata.key': trip_key,
                                                        'data.end_ts' : {'$gt': reset_ts},
                                                        'data.start_ts': {'$lt': reset_ts}
                                                       })
    if ret_place_doc is None and ret_trip_doc is None:
        raise ValueError("No trip or place straddling time %s" % reset_ts)
    if ret_place_doc is None:
        assert ret_trip_doc is not None
        ret_place_doc = esda.get_entry(esda.CLEANED_PLACE, ret_trip_doc.start_place)

    assert ret_place_doc is not None
    ret_place = ecwe.Entry(ret_place_doc)
    return ret_place

def _get_trip_key(place_key):
    if place_key == esda.CLEANED_PLACE_KEY:
        return esda.CLEANED_TRIP_KEY
    elif place_key == esda.RAW_PLACE_KEY:
        return esda.RAW_TRIP_KEY
    else:
        raise RuntimeException("Invalid place key %s" % place_key)
