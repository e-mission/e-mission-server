from __future__ import unicode_literals
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import
# Standard imports
from future import standard_library
standard_library.install_aliases()
from builtins import *
import unittest
from datetime import datetime
import logging

# Our imports
from emission.core.get_database import get_section_db
import emission.storage.decorations.useful_queries as tauq

class UsefulQueriesTests(unittest.TestCase):
    def setUp(self):
        get_section_db().delete_one({"_id": "foo_1"})
        get_section_db().delete_one({"_id": "foo_2"})
        get_section_db().delete_one({"_id": "foo_3"})

    def tearDown(self):
        get_section_db().delete_one({"_id": "foo_1"})
        get_section_db().delete_one({"_id": "foo_2"})
        get_section_db().delete_one({"_id": "foo_3"})
        self.assertEqual(get_section_db().count_documents({'_id': 'foo_1'}), 0)
        self.assertEqual(get_section_db().count_documents({'_id': 'foo_2'}), 0)
        self.assertEqual(get_section_db().count_documents({'_id': 'foo_3'}), 0)

    def testGetAllSections(self):
        get_section_db().insert_one({"_id": "foo_1", "trip_id": "bar"})
        get_section_db().insert_one({"_id": "foo_2", "trip_id": "bar"})
        get_section_db().insert_one({"_id": "foo_3", "trip_id": "baz"})
        self.assertEqual(len(tauq.get_all_sections("foo_1")), 2)

    def testGetAllSectionsForUserDay(self):
        dt1 = datetime(2015, 1, 1, 1, 1, 1)
        dt2 = datetime(2015, 1, 1, 2, 1, 1)
        dt3 = datetime(2015, 1, 1, 3, 1, 1)
        get_section_db().insert_one({"_id": "foo_1",
            "type":"move",
            "trip_id": "trip_1",
            "section_id": 3,
            "section_start_datetime": dt1,
            "section_end_datetime": dt2})
        get_section_db().insert_one({"_id": "foo_2",
            "type":"place",
            "trip_id": "trip_2",
            "section_start_datetime": dt2,
            "section_end_datetime": dt3})
        get_section_db().insert_one({"_id": "foo_3",
            "type": "move",
            "trip_id": "trip_3",
            "section_id": 0,
            "section_start_datetime": dt3})
        self.assertEqual(tauq.get_trip_before("foo_3")["_id"], "foo_1")

    def testGetTripBefore(self):
        dt1 = datetime(2015, 1, 1, 1, 1, 1)
        dt2 = datetime(2015, 1, 1, 2, 1, 1)
        dt3 = datetime(2015, 1, 2, 3, 1, 1)
        get_section_db().insert_one({"_id": "foo_1",
            "user_id": "test_user",
            "type":"move",
            "section_id": 3,
            "section_start_datetime": dt1,
            "section_end_datetime": dt2})
        get_section_db().insert_one({"_id": "foo_2",
            "user_id": "test_user",
            "type":"place",
            "section_start_datetime": dt2,
            "section_end_datetime": dt3})
        get_section_db().insert_one({"_id": "foo_3",
            "user_id": "test_user",
            "type": "move",
            "section_id": 0,
            "section_start_datetime": dt3})
        secList = tauq.get_all_sections_for_user_day("test_user", 2015, 1, 1)
        self.assertEqual(len(secList), 1)
        self.assertEqual(secList[0]._id, "foo_1")

    def testGetBounds(self):
        dt1 = datetime(2015, 1, 1, 1, 1, 1)
        dt2 = datetime(2015, 1, 1, 2, 1, 1)
        dt3 = datetime(2015, 1, 2, 3, 1, 1)
        sectionJsonList = []
        sectionJsonList.append({"_id": "foo_1",
            "user_id": "test_user",
            "type":"move",
            "section_id": 3,
            "section_start_datetime": dt1,
            "section_end_datetime": dt2,
            "section_start_point": {"coordinates": [1,2], "type": "Point"},
            "section_end_point": {"coordinates": [3,4], "type": "Point"}})
        sectionJsonList.append({"_id": "foo_2",
            "user_id": "test_user",
            "type":"place",
            "section_start_datetime": dt2,
            "section_end_datetime": dt3,
            "section_start_point": {"coordinates": [5,6], "type": "Point"},
            "section_end_point": {"coordinates": [7,8], "type": "Point"}})
        sectionJsonList.append({"_id": "foo_3",
            "user_id": "test_user",
            "type": "move",
            "section_id": 0,
            "section_start_datetime": dt3,
            "section_start_point": {"coordinates": [9,10], "type": "Point"},
            "section_end_point": {"coordinates": [11,12], "type": "Point"}})
        bounds = tauq.get_bounds(sectionJsonList)
        self.assertEqual(bounds[0].lat, 2)
        self.assertEqual(bounds[0].lon, 1)
        self.assertEqual(bounds[1].lat, 12)
        self.assertEqual(bounds[1].lon, 11)

if __name__ == '__main__':
    import emission.tests.common as etc
    etc.configLogging()
    unittest.main()
