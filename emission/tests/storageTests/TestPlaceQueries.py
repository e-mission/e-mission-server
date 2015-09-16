# Standard imports
import unittest
import datetime as pydt
import logging
import uuid
import json

# Our imports
import emission.storage.decorations.place_queries as esdp
import emission.core.get_database as edb

class TestTripQueries(unittest.TestCase):
    def setUp(self):
        self.testUserId = uuid.uuid4()
        edb.get_place_db().remove()

    def testCreateNew(self):
        new_place = esdp.create_new_place(self.testUserId)
        self.assertIsNotNone(new_place.get_id())
        self.assertEqual(new_place.user_id, self.testUserId)

    def testSavePlace(self):
        new_place = esdp.create_new_place(self.testUserId)
        new_place.enter_ts = 5
        esdp.save_place(new_place)
        self.assertEqual(edb.get_place_db().find({"enter_ts": 5}).count(), 1)
        self.assertEqual(edb.get_place_db().find_one({"enter_ts": 5})["_id"], new_place.get_id())
        self.assertEqual(edb.get_place_db().find_one({"enter_ts": 5})["user_id"], self.testUserId)

    def testGetLastPlace(self):
        self.testSavePlace()
    
        # The place saved in the previous step has no exit_ts set, so it is the
        # last place
        new_place = esdp.get_last_place(self.testUserId)
        new_place.exit_ts = 6
        esdp.save_place(new_place)

        # Now that I have set the exit_ts and saved it, there is no last place
        new_place = esdp.get_last_place(self.testUserId)
        self.assertIsNone(new_place)

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    unittest.main()
