# Standard imports
import unittest
import datetime as pydt
import logging
import uuid
import json

# Our imports
import emission.storage.decorations.stop_queries as esds
import emission.net.usercache.abstract_usercache as enua
import emission.core.get_database as edb

class TestStopQueries(unittest.TestCase):
    def setUp(self):
        self.testUserId = uuid.uuid4()
        edb.get_stop_db().remove()
        self.test_trip_id = "test_trip_id"

    def testCreateNew(self):
        new_stop = esds.create_new_stop(self.testUserId, self.test_trip_id)
        self.assertIsNotNone(new_stop.get_id())
        self.assertEqual(new_stop.user_id, self.testUserId)
        self.assertEqual(new_stop.trip_id, self.test_trip_id)

    def testSaveStop(self):
        new_stop = esds.create_new_stop(self.testUserId, self.test_trip_id)
        new_stop.enter_ts = 5
        new_stop.exit_ts = 6
        esds.save_stop(new_stop)
        self.assertEqual(edb.get_stop_db().find({"exit_ts": 6}).count(), 1)
        self.assertEqual(edb.get_stop_db().find_one({"exit_ts": 6})["_id"], new_stop.get_id())
        self.assertEqual(edb.get_stop_db().find_one({"exit_ts": 6})["user_id"], self.testUserId)
        self.assertEqual(edb.get_stop_db().find_one({"exit_ts": 6})["trip_id"], self.test_trip_id)

    def testQueryStops(self):
        new_stop = esds.create_new_stop(self.testUserId, self.test_trip_id)
        new_stop.enter_ts = 5
        new_stop.exit_ts = 6
        esds.save_stop(new_stop)
        ret_arr_one = esds.get_stops_for_trip(self.testUserId, self.test_trip_id)
        self.assertEqual(len(ret_arr_one), 1)
        self.assertEqual(ret_arr_one, [new_stop])
        ret_arr_list = esds.get_stops_for_trip_list(self.testUserId, [self.test_trip_id])
        self.assertEqual(ret_arr_one, ret_arr_list)
        ret_arr_time = esds.get_stops(self.testUserId, enua.UserCache.TimeQuery("enter_ts", 4, 6))
        self.assertEqual(ret_arr_list, ret_arr_time)

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    unittest.main()
