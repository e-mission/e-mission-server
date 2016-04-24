import logging

import emission.core.wrapper.entry as ecwe
import emission.storage.timeseries.abstract_timeseries as esta


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
