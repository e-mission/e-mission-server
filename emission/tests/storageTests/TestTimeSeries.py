# Standard imports
import unittest
import datetime as pydt
import logging
import json

# Our imports
import emission.core.get_database as edb
import emission.storage.timeseries.timequery as estt
import emission.storage.timeseries.tcquery as esttc
import emission.storage.timeseries.abstract_timeseries as esta

import emission.core.wrapper.localdate as ecwl

# Test imports
import emission.tests.common as etc

class TestTimeSeries(unittest.TestCase):
    def setUp(self):
        etc.setupRealExample(self, "emission/tests/data/real_examples/shankari_2015-aug-27")

    def tearDown(self):
        edb.get_timeseries_db().remove({"user_id": self.testUUID}) 

    def testGetUUIDList(self):
        uuid_list = esta.TimeSeries.get_uuid_list()
        self.assertIn(self.testUUID, uuid_list)

    def testGetEntries(self):
        ts = esta.TimeSeries.get_time_series(self.testUUID)
        tq = estt.TimeQuery("metadata.write_ts", 1440658800, 1440745200)
        self.assertEqual(len(list(ts.find_entries(time_query = tq))), len(self.entries))

    def testComponentQuery(self):
        ts = esta.TimeSeries.get_time_series(self.testUUID)
        tq = esttc.TimeComponentQuery("metadata.write_local_dt",
            ecwl.LocalDate({"hour": 8}), ecwl.LocalDate({"hour":9}))
        self.assertEqual(len(list(ts.find_entries(time_query = tq))), 490)

    def testGetEntryAtTs(self):
        ts = esta.TimeSeries.get_time_series(self.testUUID)
        entry_doc = ts.get_entry_at_ts("background/filtered_location", "data.ts", 1440688739.672)
        self.assertEqual(entry_doc["data"]["latitude"], 37.393415)
        self.assertEqual(entry_doc["data"]["accuracy"], 43.5)

    def testGetMaxValueForField(self):
        ts = esta.TimeSeries.get_time_series(self.testUUID)
        self.assertEqual(ts.get_max_value_for_field("background/filtered_location", "data.ts"), 1440729334.797)

    def testGetDataDf(self):
        ts = esta.TimeSeries.get_time_series(self.testUUID)
        tq = estt.TimeQuery("metadata.write_ts", 1440658800, 1440745200)
        df = ts.get_data_df("background/filtered_location", tq)
        self.assertEqual(len(df), 327)
        logging.debug("df.columns = %s" % df.columns)
        self.assertEqual(len(df.columns), 12)
        
if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    unittest.main()
