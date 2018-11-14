from __future__ import unicode_literals
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import
# Simple decorator that pulls data from a combination of the usercache and the timeseries.
# We could avoid this if we put data directly into the timeseries when it came in instead of
# buffering
# but that puts more processing into the push thread, which might be a problem for the 30s
# time window on iOS. At any rate, we would need to test that before we make that change.
# We may also be able to remove this if we run the pipeline more frequently, but would
# need to test that

from future import standard_library
standard_library.install_aliases()
from builtins import *

import emission.core.get_database as edb
import emission.net.usercache.abstract_usercache as enua
import emission.storage.timeseries.abstract_timeseries as esta

import emission.storage.timeseries.geoquery as estg
import emission.storage.timeseries.timequery as estt
import emission.storage.timeseries.tcquery as esttc

def find_entries(uuid, key_list=None, time_query=None):
    # the usercache does not support aggreate queries, so we require
    # a UUID to return combined values. Similarly, we don't support
    # any queries other than time queries
    assert(uuid is not None)
    ts = esta.TimeSeries.get_time_series(uuid)
    uc = enua.UserCache.getUserCache(uuid)
    ts_entries = ts.find_entries(key_list, time_query,
                                 geo_query = None,
                                 extra_query_list = None)
    uc_entries = uc.getMessage(key_list, time_query)
    return list(ts_entries) + uc_entries

def insert_entries(uuid, entry_it):
    # We want to get the references to the databases upfront, because
    # otherwise, we will get a new connection for each reference, which
    # will slow things down a lot
    # See
    # https://github.com/e-mission/e-mission-server/commit/aed451bc41ee09a9ff11f350881c320557fea71b
    # for details
    # This is also the reason why we pass in an iterator of entries instead of
    # one entry at a time. We don't want the interface to contain references to
    # the databases, since they are an implementation detail, and opening a
    # connection to the database for every call
    ts = esta.TimeSeries.get_time_series(uuid)
    ucdb = edb.get_usercache_db()
    tsdb_count = 0
    ucdb_count = 0
    for entry in entry_it:
        assert entry["user_id"] is not None, "user_id for entry %s is None, cannot insert" % entry
        if "write_fmt_time" in entry["metadata"]:
            # write_fmt_time is filled in only during the formatting process
            # so if write_fmt_time exists, it must be in the timeseries already
            ts.insert(entry)
            tsdb_count = tsdb_count + 1
        else:
            ucdb.insert_one(entry)
            ucdb_count = ucdb_count + 1

    return (tsdb_count, ucdb_count)
