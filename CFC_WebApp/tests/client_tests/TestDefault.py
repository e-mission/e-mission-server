import unittest
import json
from utils import load_database_json, purge_database_json
from clients.default import default
import logging
from get_database import get_db, get_mode_db, get_section_db
from datetime import datetime, timedelta
from dao.user import User

logging.basicConfig(level=logging.DEBUG)

class TestDefault(unittest.TestCase):
    def setUp(self):
        import tests.common
        # Sometimes, we may have entries left behind in the database if one of the tests failed
        # or threw an exception, so let us start by cleaning up all entries
        tests.common.dropAllCollections(get_db())
        user = User.register("fake@fake.com")
        self.uuid = user.uuid

    def testCarbonFootprintStore(self):
        user = User.fromUUID(self.uuid)
        # Tuple of JSON objects, similar to the real footprint
        dummyCarbonFootprint = ({'myModeShareCount': 10}, {'avgModeShareCount': 20})
        self.assertEquals(default.getCarbonFootprint(user), None)
        default.setCarbonFootprint(user, dummyCarbonFootprint)
        # recall that pymongo converts tuples to lists somewhere down the line
        self.assertEquals(default.getCarbonFootprint(user), list(dummyCarbonFootprint))

if __name__ == '__main__':
    unittest.main()
