# Simple decorator that pulls data from a combination of the usercache and the timeseries.
# We could avoid this if we put data directly into the timeseries when it came in instead of
# buffering
# but that puts more processing into the push thread, which might be a problem for the 30s
# time window on iOS. At any rate, we would need to test that before we make that change.
# We may also be able to remove this if we run the pipeline more frequently, but would
# need to test that

import emission.net.usercache.abstract_usercache as enua
import emission.storage.timeseries.abstract_timeseries as esta

import emission.storage.timeseries.geoquery as estg
import emission.storage.timeseries.timequery as estt
import emission.storage.timeseries.tcquery as esttc

def find_entries(uuid, key_list=None, time_query=None, limit=None):
    # the usercache does not support aggreate queries, so we require
    # a UUID to return combined values. Similarly, we don't support
    # any queries other than time queries
    assert(uuid is not None)
    ts = esta.Timeseries.get_time_series(uuid)
    uc = enua.UserCache.getUserCache(uuid)
    ts_entries = ts.find_entries(key_list, time_query,
                                 geo_query = None,
                                 extra_query_list = None,
                                 limit=limit)
    uc_entries = uc.getMessage(key_list, time_query, limit=limit)
    return list(ts_entries) + uc_entries
