from __future__ import unicode_literals
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import
# Standard imports
from future import standard_library
standard_library.install_aliases()
from builtins import range
from builtins import *
import unittest
import datetime as pydt
import logging
import json
import uuid
import attrdict as ad
import time
import geojson as gj
import arrow

# Our imports
import emission.tests.common as etc

import emission.core.get_database as edb
import emission.net.usercache.abstract_usercache as enua
import emission.storage.timeseries.abstract_timeseries as esta
import emission.net.usercache.abstract_usercache_handler as enuah
import emission.net.api.usercache as mauc
import emission.core.wrapper.trip as ecwt
import emission.core.wrapper.localdate as ecwld

# These are the current formatters, so they are included here for testing.
# However, it is unclear whether or not we need to add other tests as we add other formatters,
# specially if they follow the same pattern.

class TestBuiltinUserCacheHandlerOutput(unittest.TestCase):
    def setUp(self):
        etc.dropAllCollections(edb._get_current_db())
        self.testUserUUID1 = uuid.uuid4()
        self.testUserUUID2 = uuid.uuid4()
        self.testUserUUIDios = uuid.uuid4()

        (self.entry_list, self.ios_entry_list) = etc.setupIncomingEntries()

        self.uc1 = enua.UserCache.getUserCache(self.testUserUUID1)
        self.uc2 = enua.UserCache.getUserCache(self.testUserUUID2)
        self.ucios = enua.UserCache.getUserCache(self.testUserUUIDios)

        self.ts1 = esta.TimeSeries.get_time_series(self.testUserUUID1)
        self.ts2 = esta.TimeSeries.get_time_series(self.testUserUUID2)
        self.tsios = esta.TimeSeries.get_time_series(self.testUserUUIDios)

        for entry in self.entry_list:
            # Needed because otherwise we get a DuplicateKeyError while
            # inserting the mutiple copies 
            del entry["_id"]

        self.curr_ts = int(time.time())
        for offset in range(self.curr_ts - 5 * 60, self.curr_ts, 30):
            for entry in self.entry_list:
                entry["metadata"]["write_ts"] = offset * 1000
            mauc.sync_phone_to_server(self.testUserUUID1, self.entry_list)

        for offset in range(self.curr_ts - 7 * 60 + 1, self.curr_ts - 2 * 60 + 1, 30):
            for entry in self.entry_list:
                entry["metadata"]["write_ts"] = offset * 1000
            mauc.sync_phone_to_server(self.testUserUUID2, self.entry_list)

        for entry in self.ios_entry_list:
            # Needed because otherwise we get a DuplicateKeyError while
            # inserting the mutiple copies 
            del entry["_id"]

        for offset in range(self.curr_ts - 5 * 60, self.curr_ts, 30):
            for entry in self.ios_entry_list:
                entry["metadata"]["write_ts"] = offset
            mauc.sync_phone_to_server(self.testUserUUIDios, self.ios_entry_list)

    # The first query for every platform is likely to work 
    # because startTs = None and endTs, at least for iOS, is way out there
    # But on the next call, if we are multiplying by 1000, it won't work any more.
    # Let's add a new test for this
    def testGetLocalDay(self):
        adt = arrow.get(pydt.datetime(2016, 1, 1, 9, 46, 0, 0))
        test_dt = ecwld.LocalDate.get_local_date(adt.timestamp, "America/Los_Angeles")
        test_trip = ecwt.Trip({'start_local_dt': test_dt, 'start_fmt_time': adt.isoformat()})
        test_handler = enuah.UserCacheHandler.getUserCacheHandler(self.testUserUUID1)
        self.assertEqual(test_handler.get_local_day_from_fmt_time(test_trip), "2016-01-01")
        self.assertEqual(test_handler.get_local_day_from_local_dt(test_trip), "2016-01-01")

    def testGetTripListForSevenDays(self):
        test_handler = enuah.UserCacheHandler.getUserCacheHandler(self.testUserUUID1)

    def testGetObsoleteEntries(self):
        valid_entries = ["2015-12-30", "2015-12-29", "2015-12-31", "2015-01-01"]
        uc = enua.UserCache.getUserCache(self.testUserUUID1)
        uch = enuah.UserCacheHandler.getUserCacheHandler(self.testUserUUID1)
        uc.putDocument("2015-12-30", {"a": 1})
        uc.putDocument("2015-12-29", {"a": 1})
        uc.putDocument("2015-12-28", {"a": 1})
        uc.putDocument("2015-12-27", {"a": 1})
        uc.putDocument("2015-12-26", {"a": 1})
        obsolete_entries = uch.get_obsolete_entries(uc, valid_entries)
        # the result should include entries that are in the past (28,27,26), but should 
        # NOT include newly added entries
        self.assertEqual(obsolete_entries, set(["2015-12-28", "2015-12-27", "2015-12-26"]))

    def testDeleteObsoleteEntries(self):
        valid_bins = {"2015-12-30":[{"b": 2}],
                      "2015-12-29":[{"b": 2}],
                      "2015-12-31":[{"b": 2}]}
        uc = enua.UserCache.getUserCache(self.testUserUUID1)
        uch = enuah.UserCacheHandler.getUserCacheHandler(self.testUserUUID1)
        uc.putDocument("2015-12-30", {"a": 1})
        uc.putDocument("2015-12-29", {"a": 1})
        uc.putDocument("2015-12-28", {"a": 1})
        uc.putDocument("2015-12-27", {"a": 1})
        uc.putDocument("2015-12-26", {"a": 1})
        uch.delete_obsolete_entries(uc, list(valid_bins.keys()))
        # the result should include entries that are in the past (28,27,26), but should 
        # NOT include newly added entries
        self.assertEqual(uc.getDocumentKeyList(), ["2015-12-30", "2015-12-29"])

    def testRetainSetConfig(self):
        valid_bins = {"2015-12-30":[{"b": 2}],
                      "2015-12-29":[{"b": 2}],
                      "2015-12-31":[{"b": 2}]}
        uc = enua.UserCache.getUserCache(self.testUserUUID1)
        uch = enuah.UserCacheHandler.getUserCacheHandler(self.testUserUUID1)
        uc.putDocument("2015-12-30", {"a": 1})
        uc.putDocument("2015-12-29", {"a": 1})
        uc.putDocument("2015-12-28", {"a": 1})
        uc.putDocument("2015-12-27", {"a": 1})
        uc.putDocument("2015-12-26", {"a": 1})
        uc.putDocument("config/sensor_config", {"a": 1})
        uch.delete_obsolete_entries(uc, list(valid_bins.keys()))
        # the result should include entries that are in the past (28,27,26), but should 
        # NOT include newly added entries
        self.assertEqual(uc.getDocumentKeyList(), ["2015-12-30", "2015-12-29",
            "config/sensor_config"])

if __name__ == '__main__':
    import emission.tests.common as etc

    etc.configLogging()
    unittest.main()
