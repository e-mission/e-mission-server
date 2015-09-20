# Standard imports
import unittest
import datetime as pydt
import logging
import uuid
import json

# Our imports
import emission.storage.decorations.section_queries as esds
import emission.net.usercache.abstract_usercache as enua
import emission.core.get_database as edb

class TestSectionQueries(unittest.TestCase):
    def setUp(self):
        self.testUserId = uuid.uuid4()
        edb.get_section_new_db().remove()
        self.test_trip_id = "test_trip_id"

    def testCreateNew(self):
        new_section = esds.create_new_section(self.testUserId, self.test_trip_id)
        self.assertIsNotNone(new_section.get_id())
        self.assertEqual(new_section.user_id, self.testUserId)
        self.assertEqual(new_section.trip_id, self.test_trip_id)

    def testSaveSection(self):
        new_section = esds.create_new_section(self.testUserId, self.test_trip_id)
        new_section.start_ts = 5
        new_section.end_ts = 6
        esds.save_section(new_section)
        self.assertEqual(edb.get_section_new_db().find({"end_ts": 6}).count(), 1)
        self.assertEqual(edb.get_section_new_db().find_one({"end_ts": 6})["_id"], new_section.get_id())
        self.assertEqual(edb.get_section_new_db().find_one({"end_ts": 6})["user_id"], self.testUserId)
        self.assertEqual(edb.get_section_new_db().find_one({"end_ts": 6})["trip_id"], self.test_trip_id)

    def testQuerySections(self):
        new_section = esds.create_new_section(self.testUserId, self.test_trip_id)
        new_section.start_ts = 5
        new_section.end_ts = 6
        esds.save_section(new_section)
        ret_arr_one = esds.get_sections_for_trip(self.testUserId, self.test_trip_id)
        self.assertEqual(len(ret_arr_one), 1)
        self.assertEqual(ret_arr_one, [new_section])
        ret_arr_list = esds.get_sections_for_trip_list(self.testUserId, [self.test_trip_id])
        self.assertEqual(ret_arr_one, ret_arr_list)
        ret_arr_time = esds.get_sections(self.testUserId, enua.UserCache.TimeQuery("start_ts", 4, 6))
        self.assertEqual(ret_arr_list, ret_arr_time)

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    unittest.main()
