# Standard imports
import unittest
import datetime as pydt
import logging
import uuid
import json

# Our imports
import emission.core.get_database as edb
import emission.net.usercache.abstract_usercache as enua
import emission.storage.timeseries.abstract_timeseries as esta

class TestTimeSeries(unittest.TestCase):
    def setUp(self):
        self.entries = json.load(open("emission/tests/data/my_data_jul_22.txt"))
        self.testUUID = uuid.uuid4()
        for entry in self.entries:
            entry["user_id"] = self.testUUID
            edb.get_timeseries_db().save(entry)

    def tearDown(self):
        edb.get_timeseries_db().remove({"user_id": self.testUUID}) 

    def testGetUUIDList(self):
        uuid_list = esta.TimeSeries.get_uuid_list()
        self.assertIn(self.testUUID, uuid_list)

    def testGetEntries(self):
        ts = esta.TimeSeries.get_time_series(self.testUUID)
        tq = enua.UserCache.TimeQuery("write_ts", 1440201600, 1440288000)
        self.assertEqual(len(list(ts.find_entries(time_query = tq))), len(self.entries))

    def testGetDataDf(self):
        ts = esta.TimeSeries.get_time_series(self.testUUID)
        tq = enua.UserCache.TimeQuery("write_ts", 1440201600, 1440288000)
        df = ts.get_data_df("background/filtered_location", tq)
        self.assertEqual(len(df), 583)
        self.assertEqual(len(df.columns), 9)
        
if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    unittest.main()
