# Standard imports
import unittest
import datetime as pydt
import logging
import json
import uuid
import attrdict as ad
import time

# Our imports
import emission.tests.common

import emission.core.get_database as edb
import emission.net.usercache.abstract_usercache as enua
import emission.storage.timeseries.abstract_timeseries as esta
import emission.net.usercache.abstract_usercache_handler as enuah
import emission.net.api.usercache as mauc

# These are the current formatters, so they are included here for testing.
# However, it is unclear whether or not we need to add other tests as we add other formatters,
# specially if they follow the same pattern.

class TestBuiltinUserCacheHandler(unittest.TestCase):
    def setUp(self):
        emission.tests.common.dropAllCollections(edb.get_db())
        self.testUserUUID1 = uuid.uuid4()
        self.testUserUUID2 = uuid.uuid4()
        self.testUserUUIDios = uuid.uuid4()
        
        self.activity_entry = json.load(open("emission/tests/data/netTests/android.activity.txt"))
        self.location_entry = json.load(open("emission/tests/data/netTests/android.location.raw.txt"))
        self.transition_entry = json.load(open("emission/tests/data/netTests/android.transition.txt"))
        self.entry_list = [self.activity_entry, self.location_entry, self.transition_entry]

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
                entry["metadata"]["write_ts"] = self.curr_ts - offset
            mauc.sync_phone_to_server(self.testUserUUID1, self.entry_list)

        for offset in range(self.curr_ts - 7 * 60 + 1, self.curr_ts - 2 * 60 + 1, 30):
            for entry in self.entry_list:
                entry["metadata"]["write_ts"] = self.curr_ts - offset
            mauc.sync_phone_to_server(self.testUserUUID2, self.entry_list)
            
        self.ios_activity_entry = json.load(open("emission/tests/data/netTests/ios.activity.txt"))
        self.ios_location_entry = json.load(open("emission/tests/data/netTests/ios.location.txt"))
        self.ios_transition_entry = json.load(open("emission/tests/data/netTests/ios.transition.txt"))
        self.ios_entry_list = [self.ios_activity_entry, self.ios_location_entry, self.ios_transition_entry]
        for entry in self.ios_entry_list:
            # Needed because otherwise we get a DuplicateKeyError while
            # inserting the mutiple copies 
            del entry["_id"]

        for offset in range(self.curr_ts - 5 * 60, self.curr_ts, 30):
            for entry in self.ios_entry_list:
                entry["metadata"]["write_ts"] = self.curr_ts - offset
            mauc.sync_phone_to_server(self.testUserUUIDios, self.ios_entry_list)


    def testMoveToLongTerm(self):
        # 5 mins of data, every 30 secs = 10 entries per entry type. There are
        # 3 entry types, so 30 entries

        # First all the entries are in the usercache
        self.assertEqual(len(self.uc1.getMessage()), 30)
        self.assertEqual(len(list(self.ts1.find_entries())), 0)

        self.assertEqual(len(self.uc2.getMessage()), 30)
        self.assertEqual(len(list(self.ts2.find_entries())), 0)
        
        self.assertEqual(len(self.ucios.getMessage()), 30)
        self.assertEqual(len(list(self.tsios.find_entries())), 0)


        # Then we move entries for user1 into longterm
        enuah.UserCacheHandler.getUserCacheHandler(self.testUserUUID1).moveToLongTerm()

        # So we end up with all user1 entries in longterm
        self.assertEqual(len(self.uc1.getMessage()), 0)
        self.assertEqual(len(list(self.ts1.find_entries())), 30)
        
        # Then, we move entries for the ios user into longterm
        enuah.UserCacheHandler.getUserCacheHandler(self.testUserUUIDios).moveToLongTerm()
        
        self.assertEqual(len(self.ucios.getMessage()), 0)
        self.assertEqual(len(list(self.tsios.find_entries())), 30)
        
        # 30 entries from android + 30 entries from ios = 60
        self.assertEqual(edb.get_timeseries_db().find().count(), 60)
        self.assertEqual(edb.get_timeseries_error_db().find().count(), 0)

        # But all existing entries still in usercache for the second user
        self.assertEqual(len(self.uc2.getMessage()), 30)
        self.assertEqual(len(list(self.ts2.find_entries())), 0)

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    unittest.main()
