import unittest
import json
from utils import load_database_json, purge_database_json
from clients.choice import choice
import logging
from get_database import get_db, get_mode_db, get_section_db
from datetime import datetime, timedelta
from dao.user import User

logging.basicConfig(level=logging.DEBUG)

class TestChoice(unittest.TestCase):
    def setUp(self):
        import tests.common
        # Sometimes, we may have entries left behind in the database if one of the tests failed
        # or threw an exception, so let us start by cleaning up all entries
        tests.common.dropAllCollections(get_db())
        user = User.register("fake@fake.com")
        self.uuid = user.uuid

    def testCurrView(self):
        self.assertEquals(choice.getCurrView(self.uuid), "data")
        choice.setCurrView(self.uuid, "game")
        self.assertEquals(choice.getCurrView(self.uuid), "game")

    def testSwitchResultDisplay(self):
        self.assertEquals(choice.getCurrView(self.uuid), "data")

        params = {"client_key": "this_is_the_super_secret_id",
                  "uuid": str(self.uuid),
                  "new_view": "game"}
        choice.switchResultDisplay(params)

        self.assertEquals(choice.getCurrView(self.uuid), "game")

if __name__ == '__main__':
    unittest.main()
