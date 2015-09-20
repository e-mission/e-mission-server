# Standard imports
import unittest
import datetime as pydt
import logging
import uuid
import json

# Our imports
import emission.storage.decorations.trip_queries as esdt
import emission.storage.decorations.section_queries as esds
import emission.storage.decorations.stop_queries as esdst
import emission.net.usercache.abstract_usercache as enua
import emission.core.get_database as edb

class TestTripQueries(unittest.TestCase):
    def setUp(self):
        self.testUserId = uuid.uuid4()
        edb.get_trip_new_db().remove()

    def testCreateNew(self):
        new_trip = esdt.create_new_trip(self.testUserId)
        self.assertIsNotNone(new_trip.get_id())
        self.assertEqual(new_trip.user_id, self.testUserId)

    def testSaveTrip(self):
        new_trip = esdt.create_new_trip(self.testUserId)
        new_trip.start_ts = 5
        new_trip.end_ts = 6
        esdt.save_trip(new_trip)
        self.assertEqual(edb.get_trip_new_db().find({"end_ts": 6}).count(), 1)
        self.assertEqual(edb.get_trip_new_db().find_one({"end_ts": 6})["_id"], new_trip.get_id())
        self.assertEqual(edb.get_trip_new_db().find_one({"end_ts": 6})["user_id"], self.testUserId)

    def testQueryTrips(self):
        new_trip = esdt.create_new_trip(self.testUserId)
        new_trip.start_ts = 5
        new_trip.end_ts = 6
        esdt.save_trip(new_trip)
        ret_arr_time = esdt.get_trips(self.testUserId, enua.UserCache.TimeQuery("start_ts", 4, 6))
        self.assertEqual(ret_arr_time, [new_trip])

    def testQuerySectionsForTrip(self):
        new_trip = esdt.create_new_trip(self.testUserId)
        new_trip.start_ts = 5
        new_trip.end_ts = 6
        esdt.save_trip(new_trip)
        new_section = esds.create_new_section(self.testUserId, new_trip.get_id())
        new_section.start_ts = 5
        new_section.end_ts = 6
        esds.save_section(new_section)
        ret_sections = esdt.get_sections_for_trip(self.testUserId, new_trip.get_id())
        self.assertEqual(ret_sections, [new_section])

    def testQueryStopsForTrip(self):
        new_trip = esdt.create_new_trip(self.testUserId)
        new_trip.start_ts = 5
        new_trip.end_ts = 6
        esdt.save_trip(new_trip)
        new_stop = esdst.create_new_stop(self.testUserId, new_trip.get_id())
        new_stop.enter_ts = 5
        new_stop.exit_ts = 6
        esdst.save_stop(new_stop)
        ret_stops = esdt.get_stops_for_trip(self.testUserId, new_trip.get_id())
        self.assertEqual(ret_stops, [new_stop])

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    unittest.main()
