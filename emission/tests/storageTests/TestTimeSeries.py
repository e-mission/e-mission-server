from __future__ import unicode_literals
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import
# Standard imports
from future import standard_library
standard_library.install_aliases()
from builtins import *
import unittest
import datetime as pydt
import logging
import json
import pymongo

# Our imports
import emission.core.get_database as edb
import emission.storage.timeseries.timequery as estt
import emission.storage.timeseries.tcquery as esttc
import emission.storage.timeseries.abstract_timeseries as esta
import emission.storage.timeseries.aggregate_timeseries as estag

import emission.core.wrapper.localdate as ecwl

# Test imports
import emission.tests.common as etc

class TestTimeSeries(unittest.TestCase):
    def setUp(self):
        self.testEmail = "user1"
        etc.setupRealExample(self, "emission/tests/data/real_examples/shankari_2015-aug-21")
        self.testUUID1 = self.testUUID
        self.entries1 = self.entries

        self.testEmail = "user2"
        etc.setupRealExample(self, "emission/tests/data/real_examples/shankari_2015-aug-27")

    def tearDown(self):
        edb.get_timeseries_db().delete_many({"user_id": self.testUUID})
        edb.get_uuid_db().delete_one({"user_email": "user1"})
        edb.get_uuid_db().delete_one({"user_email": "user2"})

    def testGetUUIDList(self):
        uuid_list = esta.TimeSeries.get_uuid_list()
        self.assertIn(self.testUUID, uuid_list)

    def testGetEntries(self):
        ts = esta.TimeSeries.get_time_series(self.testUUID)
        tq = estt.TimeQuery("metadata.write_ts", 1440658800, 1440745200)
        self.assertEqual(len(list(ts.find_entries(time_query=tq))), len(self.entries))

    def testComponentQuery(self):
        ts = esta.TimeSeries.get_time_series(self.testUUID)
        tq = esttc.TimeComponentQuery("metadata.write_local_dt",
            ecwl.LocalDate({"hour": 8}), ecwl.LocalDate({"hour":9}))
        self.assertEqual(len(list(ts.find_entries(time_query=tq))), 490)

    def testGetEntryAtTs(self):
        ts = esta.TimeSeries.get_time_series(self.testUUID)
        entry_doc = ts.get_entry_at_ts("background/filtered_location", "data.ts", 1440688739.672)
        self.assertEqual(entry_doc["data"]["latitude"], 37.393415)
        self.assertEqual(entry_doc["data"]["accuracy"], 43.5)

    def testGetMaxValueForField(self):
        ts = esta.TimeSeries.get_time_series(self.testUUID)
        self.assertEqual(ts.get_first_value_for_field("background/filtered_location", "data.ts", pymongo.DESCENDING), 1440729334.797)

    def testGetDataDf(self):
        ts = esta.TimeSeries.get_time_series(self.testUUID)
        tq = estt.TimeQuery("metadata.write_ts", 1440658800, 1440745200)
        df = ts.get_data_df("background/filtered_location", tq)
        self.assertEqual(len(df), 327)
        logging.debug("df.columns = %s" % df.columns)
        self.assertEqual(len(df.columns), 21)

    def testExtraQueries(self):
        ts = esta.TimeSeries.get_time_series(self.testUUID)
        # Query for all of Aug
        tq = estt.TimeQuery("metadata.write_ts", 1438387200, 1441065600)
        ignored_phones = {"user_id": {"$nin": [self.testUUID]}}
        # user_id is in both the extra query and the base query
        with self.assertRaises(AttributeError):
            list(ts.find_entries(time_query=tq, extra_query_list=[ignored_phones]))

    def testFindEntriesCount(self):
        '''
        Test: Specific keys with other parameters not passed values.
        Input: For each dataset: ["background/location", "background/filtered_location]
            - Testing this with sample dataset: "shankari_2015-aug-21", "shankari_2015-aug-27"
        Output: Aug_21: [738, 508], Aug_27: [555, 327]
            - Actual output just returns a single number for count of entries.
            - Validated using grep count of occurrences for keys: 1) "background/location"     2) "background/filtered_location"
                - $ grep -c <key> <dataset>.json
        '''
        ts1_aug_21 = esta.TimeSeries.get_time_series(self.testUUID1)
        ts2_aug_27 = esta.TimeSeries.get_time_series(self.testUUID)

        count_ts1 = [ts1_aug_21.find_entries_count(key="background/location"), ts1_aug_21.find_entries_count(key="background/filtered_location")]
        print("\nEntry counts for location, filtered_location on {} = {}".format("Aug_21", count_ts1))
        self.assertEqual(count_ts1, [738, 508])

        count_ts2 = [ts2_aug_27.find_entries_count(key="background/location"), ts2_aug_27.find_entries_count(key="background/filtered_location")]
        print("Entry counts for location, filtered_location on {} = {}".format("Aug_27", count_ts2))
        self.assertEqual(count_ts2, [555, 327])

        print("Assert Test for Count Data successful!")
        

if __name__ == '__main__':
    import emission.tests.common as etc
    etc.configLogging()
    unittest.main()
