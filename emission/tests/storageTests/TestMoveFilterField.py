# Standard imports
import unittest
import datetime as pydt
import logging
import json

# Our imports
import emission.core.get_database as edb
import emission.storage.timeseries.format_hacks.move_filter_field as estfm

# Test imports
import emission.tests.common as etc

class TestMoveFilterField(unittest.TestCase):
    def setUp(self):
        etc.setupRealExample(self, "emission/tests/data/real_examples/iphone_2015-11-06")

    def tearDown(self):
        edb.get_timeseries_db().remove({"user_id": self.testUUID}) 

    def testMoveFilters(self):
        # First, check that all filters are in metadata

        for entry in edb.get_timeseries_db().find({'user_id': self.testUUID,
            'metadata.filter': 'distance',
            "metadata.key": "background/location"}):
            del entry["_id"]
            entry["metadata"]["key"] = "background/filtered_location"
            edb.get_timeseries_db().insert(entry)

        self.assertEquals(edb.get_timeseries_db().find({'user_id': self.testUUID,
            'metadata.filter': 'distance',
            "metadata.key": "background/location"}).count(), 474)

        self.assertEquals(edb.get_timeseries_db().find({'user_id': self.testUUID,
            'metadata.filter': 'distance',
            "metadata.key": "background/filtered_location"}).count(), 474)

        self.assertEquals(edb.get_timeseries_db().find({'user_id': self.testUUID,
            'metadata.filter': 'distance',
            "metadata.key": "background/motion_activity"}).count(), 594)

        self.assertEquals(edb.get_timeseries_db().find({'user_id': self.testUUID,
            'metadata.filter': 'distance',
            "metadata.key": "statemachine/transition"}).count(), 20)

        # Now, move all filters
        estfm.move_all_filters_to_data()

        # Finally, check that no filters are in metadata
        self.assertEquals(edb.get_timeseries_db().find({'user_id': self.testUUID,
            'metadata.filter': 'distance',
            "metadata.key": "background/location"}).count(), 0)

        self.assertEquals(edb.get_timeseries_db().find({'user_id': self.testUUID,
            'metadata.filter': 'distance',
            "metadata.key": "background/filtered_location"}).count(), 0)

        self.assertEquals(edb.get_timeseries_db().find({'user_id': self.testUUID,
            'metadata.filter': 'distance',
            "metadata.key": "background/motion_activity"}).count(), 0)

        self.assertEquals(edb.get_timeseries_db().find({'user_id': self.testUUID,
            'metadata.filter': 'distance',
            "metadata.key": "statemachine/transition"}).count(), 0)

        # And that location filters are in data
        self.assertEquals(edb.get_timeseries_db().find({'user_id': self.testUUID,
            'data.filter': 'distance',
            "metadata.key": "background/location"}).count(), 474)

        self.assertEquals(edb.get_timeseries_db().find({'user_id': self.testUUID,
            'data.filter': 'distance',
            "metadata.key": "background/filtered_location"}).count(), 474)

        # But not in the others
        self.assertEquals(edb.get_timeseries_db().find({'user_id': self.testUUID,
            'data.filter': 'distance',
            "metadata.key": "background/motion_activity"}).count(), 0)

        self.assertEquals(edb.get_timeseries_db().find({'user_id': self.testUUID,
            'data.filter': 'distance',
            "metadata.key": "statemachine/transition"}).count(), 0)

    def testInsertFilters(self):
        for entry in edb.get_timeseries_db().find({'user_id': self.testUUID,
            'metadata.filter': 'distance',
            "metadata.key": "background/location"}):
            del entry["_id"]
            del entry["metadata"]["filter"]
            entry["metadata"]["key"] = "background/filtered_location"
            edb.get_timeseries_db().insert(entry)

        # At this point, all the filtered_location entries will not have any filters
        self.assertEquals(edb.get_timeseries_db().find({'user_id': self.testUUID,
            'metadata.filter': 'distance',
            "metadata.key": "background/filtered_location"}).count(), 0)

        self.assertEquals(edb.get_timeseries_db().find({'user_id': self.testUUID,
            'metadata.filter': 'time',
            "metadata.key": "background/filtered_location"}).count(), 0)

        # Now, move all filters
        estfm.move_all_filters_to_data()

        # The entries should now be set to "time"
        # At this point, all the filtered_location entries will not have any filters
        self.assertEquals(edb.get_timeseries_db().find({'user_id': self.testUUID,
            'data.filter': 'distance',
            "metadata.key": "background/filtered_location"}).count(), 0)

        self.assertEquals(edb.get_timeseries_db().find({'user_id': self.testUUID,
            'data.filter': 'time',
            "metadata.key": "background/filtered_location"}).count(), 474)

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    unittest.main()
