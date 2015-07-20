# Standard imports
import unittest
import json
import logging
from datetime import datetime, timedelta

# Our imports
from emission.core.get_database import get_db, get_mode_db, get_section_db
from emission.tests.analysisTests.result_precompute import precompute_results
from emission.core.wrapper.user import User
from emission.core.wrapper.client import Client
import tests.common
from emission.clients.testclient import testclient
from emission.clients.data import data

logging.basicConfig(level=logging.DEBUG)

class TestPrecomputeResults(unittest.TestCase):
    def setUp(self):
        self.testUsers = ["test@example.com", "best@example.com", "fest@example.com",
                          "rest@example.com", "nest@example.com"]
        self.serverName = 'localhost'

        # Sometimes, we may have entries left behind in the database if one of the tests failed
        # or threw an exception, so let us start by cleaning up all entries
        tests.common.dropAllCollections(get_db())

        self.ModesColl = get_mode_db()
        self.assertEquals(self.ModesColl.find().count(), 0)

        self.SectionsColl = get_section_db()
        self.assertEquals(self.SectionsColl.find().count(), 0)

        tests.common.loadTable(self.serverName, "Stage_Modes", "tests/data/modes.json")
        tests.common.loadTable(self.serverName, "Stage_Sections", "tests/data/testModeInferFile")

        # Let's make sure that the users are registered so that they have profiles
        for userEmail in self.testUsers:
          User.register(userEmail)

        self.now = datetime.now()
        self.dayago = self.now - timedelta(days=1)
        self.weekago = self.now - timedelta(weeks = 1)

        for section in self.SectionsColl.find():
          section['section_start_datetime'] = self.dayago
          section['section_end_datetime'] = self.dayago + timedelta(hours = 1)
          if (section['confirmed_mode'] == 5):
            # We only cluster bus and train trips
            # And our test data only has bus trips
            section['section_start_point'] = {u'type': u'Point', u'coordinates': [-122.270039042, 37.8800285728]}
            section['section_end_point'] = {u'type': u'Point', u'coordinates': [-122.2690412952, 37.8739578595]}
          # print("Section start = %s, section end = %s" %
          #   (section['section_start_datetime'], section['section_end_datetime']))
          # Replace the user email with the UUID
          section['user_id'] = User.fromEmail(section['user_id']).uuid
          self.SectionsColl.save(section)
          self.pr = precompute_results.PrecomputeResults()

    def testClientSpecificPrecompute(self):
        for email in self.testUsers:
            currUser = User.fromEmail(email)
            self.assertEqual(currUser.getProfile().get("testfield1"), None)
            self.assertEqual(currUser.getProfile().get("testfield2"), None)
            self.assertEqual(data.getCarbonFootprint(currUser), None)

        fakeEmail = "fest@example.com"

        client = Client("testclient")
        client.update(createKey = False)
        tests.common.makeValid(client)

        (resultPre, resultReg) = client.preRegister("this_is_the_super_secret_id", fakeEmail)
        user = User.fromEmail(fakeEmail)
        self.assertEqual(user.getFirstStudy(), 'testclient')

        self.pr.precomputeResults()

        self.assertEqual(user.getProfile()['testfield1'], 'value1')
        self.assertEqual(user.getProfile()['testfield2'], 'value2')

        for email in self.testUsers:
            if email != fakeEmail:
                currUser = User.fromEmail(email)

                carbonFootprint = data.getCarbonFootprint(currUser)
                self.assertEqual(len(carbonFootprint), 12)

if __name__ == '__main__':
    unittest.main()
