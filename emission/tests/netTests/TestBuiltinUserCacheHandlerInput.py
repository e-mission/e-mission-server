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
import bson.objectid as boi

# Our imports
import emission.tests.common

import emission.core.get_database as edb
import emission.net.usercache.abstract_usercache as enua
import emission.storage.timeseries.abstract_timeseries as esta
import emission.net.usercache.abstract_usercache_handler as enuah
import emission.net.api.usercache as mauc
import emission.core.wrapper.trip as ecwt

# These are the current formatters, so they are included here for testing.
# However, it is unclear whether or not we need to add other tests as we add other formatters,
# specially if they follow the same pattern.

class TestBuiltinUserCacheHandlerInput(unittest.TestCase):
    def setUp(self):
        emission.tests.common.dropAllCollections(edb._get_current_db())
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
                entry["metadata"]["write_ts"] = offset
            mauc.sync_phone_to_server(self.testUserUUID1, self.entry_list)

        for offset in range(self.curr_ts - 7 * 60 + 1, self.curr_ts - 2 * 60 + 1, 30):
            for entry in self.entry_list:
                entry["metadata"]["write_ts"] = offset
            mauc.sync_phone_to_server(self.testUserUUID2, self.entry_list)

        for entry in self.ios_entry_list:
            # Needed because otherwise we get a DuplicateKeyError while
            # inserting the mutiple copies 
            del entry["_id"]

        for offset in range(self.curr_ts - 5 * 60, self.curr_ts, 30):
            for entry in self.ios_entry_list:
                entry["metadata"]["write_ts"] = offset
            mauc.sync_phone_to_server(self.testUserUUIDios, self.ios_entry_list)

    def tearDown(self):
        edb.get_usercache_db().remove({"user_id": self.testUserUUID1})
        edb.get_usercache_db().remove({"user_id": self.testUserUUID2})
        edb.get_usercache_db().remove({"user_id": self.testUserUUIDios})

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

    def testMoveWhenEmpty(self):
        # 5 mins of data, every 30 secs = 10 entries per entry type. There are
        # 3 entry types, so 30 entries

        # First all the entries are in the usercache
        self.assertEqual(len(self.uc1.getMessage()), 30)
        self.assertEqual(len(list(self.ts1.find_entries())), 0)

        # Then we move entries for user1 into longterm
        enuah.UserCacheHandler.getUserCacheHandler(self.testUserUUID1).moveToLongTerm()

        # So we end up with all user1 entries in longterm
        self.assertEqual(len(self.uc1.getMessage()), 0)
        self.assertEqual(len(list(self.ts1.find_entries())), 30)

        # Add an invalid type
        edb.get_usercache_db().insert({
            'user_id': self.testUserUUID1,
            '_id': boi.ObjectId('572d3621d282b8f30def7e85'),
            'data': {u'transition': None,
                     'currState': u'STATE_ONGOING_TRIP'},
            'metadata': {'plugin': 'none',
                         'write_ts': self.curr_ts - 25,
                         'time_zone': u'America/Los_Angeles',
                         'platform': u'ios',
                         'key': u'statemachine/transition',
                         'read_ts': self.curr_ts - 27,
                         'type': u'message'}})


        # Re-run long-term for the user
        enuah.UserCacheHandler.getUserCacheHandler(self.testUserUUID1).moveToLongTerm()

        # That was stored in error_db, no errors in main body
        self.assertEqual(edb.get_timeseries_error_db().find({"user_id": self.testUserUUID1}).count(), 1)
        self.assertEqual(len(self.uc1.getMessage()), 0)
        self.assertEqual(len(list(self.ts1.find_entries())), 30)

    def testMoveDuplicateKey(self):
        # 5 mins of data, every 30 secs = 10 entries per entry type. There are
        # 3 entry types, so 30 entries

        # First all the entries are in the usercache
        self.assertEqual(len(self.uc1.getMessage()), 30)
        self.assertEqual(len(list(self.ts1.find_entries())), 0)

        # Store the entries before the move so that we can duplicate them later
        entries_before_move = self.uc1.getMessage()

        # Then we move entries for user1 into longterm
        enuah.UserCacheHandler.getUserCacheHandler(self.testUserUUID1).moveToLongTerm()

        # So we end up with all user1 entries in longterm
        self.assertEqual(len(self.uc1.getMessage()), 0)
        self.assertEqual(len(list(self.ts1.find_entries())), 30)

        # Put the same entries (with the same object IDs into the cache again)
        edb.get_usercache_db().insert(entries_before_move)
        self.assertEqual(len(self.uc1.getMessage()), 30)

        self.assertEqual(len(self.uc2.getMessage()), 30)
        # Also reset the user2 cache to be user1 so that we have a fresh supply of entries
        update_result = edb.get_usercache_db().update({"user_id": self.testUserUUID2},
                                      {"$set": {"user_id": self.testUserUUID1}},
                                      multi=True)
        logging.debug("update_result = %s" % update_result)

        # Now, we should have 60 entries in the usercache (30 duplicates + 30 from user2)
        self.assertEqual(len(self.uc1.getMessage()), 60)
        self.assertEqual(len(list(self.ts1.find_entries())), 30)

        edb.get_pipeline_state_db().remove({"user_id": self.testUserUUID1})

        # Then we move entries for user1 into longterm again
        enuah.UserCacheHandler.getUserCacheHandler(self.testUserUUID1).moveToLongTerm()

        # All the duplicates should have been ignored, and the new entries moved into the timeseries
        self.assertEqual(len(self.uc1.getMessage()), 0)
        self.assertEqual(len(list(self.ts1.find_entries())), 60)

    # The first query for every platform is likely to work 
    # because startTs = None and endTs, at least for iOS, is way out there
    # But on the next call, if we are multiplying by 1000, it won't work any more.
    # Let's add a new test for this
    def testTwoLongTermCalls(self):
        # First all the entries are in the usercache
        self.assertEqual(len(self.uc1.getMessage()), 30)
        self.assertEqual(len(list(self.ts1.find_entries())), 0)
        
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

        logging.debug("old curr_ts = %d" % self.curr_ts)
        time.sleep(20)
        self.curr_ts = int(time.time())
        logging.debug("new curr_ts = %d" % self.curr_ts)
        
        # Now, re-insert entries for android user1 and ios user1
        for offset in range(self.curr_ts - 10, self.curr_ts):
            for entry in self.entry_list:
                entry["metadata"]["write_ts"] = offset * 1000
            mauc.sync_phone_to_server(self.testUserUUID1, self.entry_list)

        for offset in range(self.curr_ts - 10, self.curr_ts):
            for entry in self.ios_entry_list:
                entry["metadata"]["write_ts"] = offset
            mauc.sync_phone_to_server(self.testUserUUIDios, self.ios_entry_list)
            
        # Now, repeat the above tests to ensure that they get moved again
        self.assertEqual(len(self.uc1.getMessage()), 30)
        # We will already have 30 entries in long-term for both android
        self.assertEqual(len(list(self.ts1.find_entries())), 30)
        
        self.assertEqual(len(self.ucios.getMessage()), 30)
        # and ios
        self.assertEqual(len(list(self.tsios.find_entries())), 30)

        # The timequery is 5 secs into the past, to avoid races
        # So let's sleep here for 5 secs
        time.sleep(5)

        # Then we move entries for user1 into longterm
        enuah.UserCacheHandler.getUserCacheHandler(self.testUserUUID1).moveToLongTerm()

        # Now, we have two sets of entries, so we will have 60 entries in longterm
        self.assertEqual(len(self.uc1.getMessage()), 0)
        self.assertEqual(len(list(self.ts1.find_entries())), 60)
        
        # Then, we move entries for the ios user into longterm
        enuah.UserCacheHandler.getUserCacheHandler(self.testUserUUIDios).moveToLongTerm()
        
        self.assertEqual(len(self.ucios.getMessage()), 0)
        self.assertEqual(len(list(self.tsios.find_entries())), 60)
        
        # 60 entries from android + 60 entries from ios = 120
        self.assertEqual(edb.get_timeseries_db().find().count(), 120)
        self.assertEqual(edb.get_timeseries_error_db().find().count(), 0)

if __name__ == '__main__':
    import emission.tests.common as etc

    etc.configLogging()
    unittest.main()
