# Standard imports
import unittest
import datetime as pydt
import logging
import uuid
import json

# Our imports
import emission.storage.decorations.trip_queries as esdt
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

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    unittest.main()
