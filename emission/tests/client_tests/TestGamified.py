import unittest
import json
from utils import load_database_json
from clients.gamified import gamified
import logging
from get_database import get_db, get_mode_db, get_section_db
from datetime import datetime, timedelta

logging.basicConfig(level=logging.DEBUG)

class TestGamified(unittest.TestCase):
    def setUp(self):
        import tests.common
        from copy import copy

        self.testUsers = ["test@example.com", "best@example.com", "fest@example.com",
                      "rest@example.com", "nest@example.com"]
        self.serverName = 'localhost'

        # Sometimes, we may have entries left behind in the database if one of the tests failed
        # or threw an exception, so let us start by cleaning up all entries
        tests.common.dropAllCollections(get_db())
        self.ModesColl = get_mode_db()
        self.assertEquals(self.ModesColl.find().count(), 0)

        self.setupUserAndClient()

        load_database_json.loadTable(self.serverName, "Stage_Modes", "tests/data/modes.json")
        load_database_json.loadTable(self.serverName, "Stage_Sections", "tests/data/testCarbonFile")
        self.SectionsColl = get_section_db()

        self.walkExpect = 1057.2524056424411
        self.busExpect = 2162.668467546699
        self.busCarbon = 267.0/1609
        self.airCarbon = 217.0/1609
        self.driveCarbon = 278.0/1609
        self.busOptimalCarbon = 92.0/1609

        self.allDriveExpect = (self.busExpect * self.driveCarbon + self.walkExpect * self.driveCarbon)/1000
        self.myFootprintExpect = float(self.busExpect * self.busCarbon)/1000
        self.sb375GoalExpect = 40.142892/7

        self.mineMinusOptimalExpect = 0
        self.allDriveMinusMineExpect = float(self.allDriveExpect - self.myFootprintExpect)/self.allDriveExpect
        self.sb375DailyGoalMinusMineExpect = float(self.sb375GoalExpect - self.myFootprintExpect)/self.sb375GoalExpect

        self.now = datetime.now()
        self.twodaysago = self.now - timedelta(days=2)
        self.weekago = self.now - timedelta(weeks = 1)

        for section in self.SectionsColl.find():
            section['section_start_datetime'] = self.twodaysago
            section['section_end_datetime'] = self.twodaysago + timedelta(hours = 1)
            section['predicted_mode'] = {'walking': 1.0}
            if section['user_id'] == 'fest@example.com':
                logging.debug("Setting user_id for section %s, %s = %s" %
                    (section['trip_id'], section['section_id'], self.user.uuid))
                section['user_id'] = self.user.uuid
            if section['confirmed_mode'] == 5:
                airSection = copy(section)
                airSection['confirmed_mode'] = 9
                airSection['_id'] = section['_id'] + "_air"
                self.SectionsColl.insert(airSection)
                airSection['confirmed_mode'] = ''
                airSection['_id'] = section['_id'] + "_unconf"
                self.SectionsColl.insert(airSection)
          
            # print("Section start = %s, section end = %s" %
            #   (section['section_start_datetime'], section['section_end_datetime']))
            self.SectionsColl.save(section)

    def setupUserAndClient(self):
        # At this point, the more important test is to execute the query and see
        # how well it works
        from dao.user import User
        from dao.client import Client
        import tests.common
        from datetime import datetime, timedelta
        from get_database import get_section_db

        fakeEmail = "fest@example.com"

        client = Client("gamified")
        client.update(createKey = False)
        tests.common.makeValid(client)

        (resultPre, resultReg) = client.preRegister("this_is_the_super_secret_id", fakeEmail)
        studyList = Client.getPendingClientRegs(fakeEmail)
        self.assertEqual(studyList, ["gamified"])

        user = User.register("fest@example.com")
        self.assertEqual(user.getFirstStudy(), 'gamified')
        self.user = user

    def testGetScoreComponents(self):
        components = gamified.getScoreComponents(self.user.uuid, self.weekago, self.now)
        self.assertEqual(components[0], 0.75)
        # bus_short disappears in optimal, air_short disappears as long motorized, so optimal = 0
        # self.assertEqual(components[1], (self.busExpect * self.busCarbon) / 1000)
        # TODO: Figure out what we should do when optimal == 0. Currently, we
        # return 0, which seems sub-optimal (pun intended)
        self.assertEqual(components[1], 0.0)
        # air_short disappears as long motorized, but we need to consider walking
        self.assertAlmostEqual(components[2], self.allDriveMinusMineExpect, places=4)
        # air_short disappears as long motorized, so only bus_short is left
        self.assertAlmostEqual(components[3], self.sb375DailyGoalMinusMineExpect, places = 4)

    # Checks both calcScore and updateScore, since we calculate the score before we update it
    def testUpdateScore(self):
        self.assertEqual(gamified.getStoredScore(self.user), (0, 0))
        components = gamified.updateScore(self.user.uuid)
        print "self.allDriveMinusMineExpect = %s, self.sb375DailyGoalMinusMineExpect = %s" % \
            (self.allDriveMinusMineExpect, self.sb375DailyGoalMinusMineExpect)
        expectedScore = 0.75 * 50 + 30 * self.allDriveMinusMineExpect + 20 * 0.0 + \
            10 * self.sb375DailyGoalMinusMineExpect
        storedScore = gamified.getStoredScore(self.user)
        self.assertEqual(storedScore[0], 0)
        self.assertAlmostEqual(storedScore[1], expectedScore, 6)

    def testGetLevel(self):
        self.assertEqual(gamified.getLevel(0), (1, 1))
        self.assertEqual(gamified.getLevel(11.0), (1, 1))
        self.assertEqual(gamified.getLevel(21.0), (1, 2))
        self.assertEqual(gamified.getLevel(100), (2, 1))
        self.assertEqual(gamified.getLevel(199.0), (2, 1))
        self.assertEqual(gamified.getLevel(200), (2, 2))
        self.assertEqual(gamified.getLevel(201.0), (2, 2))
        self.assertEqual(gamified.getLevel(999), (2, 5))
        self.assertEqual(gamified.getLevel(1000), (3, 1))
        self.assertEqual(gamified.getLevel(9999.0), (3, 5))
        self.assertEqual(gamified.getLevel(10000), (3, 5))
        self.assertEqual(gamified.getLevel(100000), (3, 5))

    def testGetFileName(self):
        self.assertEqual(gamified.getFileName(1, 1), "level_1_1.png")
        self.assertEqual(gamified.getFileName(1.0, 2.0), "level_1_2.png")
        self.assertEqual(gamified.getFileName(1.055, 2), "level_1_2.png")

    def testRunBackgroundTasksForDay(self):
        self.assertEqual(gamified.getStoredScore(self.user), (0, 0))
        components = gamified.runBackgroundTasks(self.user.uuid)
        expectedScore = 0.75 * 50 + 30 * self.allDriveMinusMineExpect + 20 * 0.0 + \
            10 * self.sb375DailyGoalMinusMineExpect
        storedScore = gamified.getStoredScore(self.user)
        self.assertEqual(storedScore[0], 0)
        self.assertAlmostEqual(storedScore[1], expectedScore, 6)

if __name__ == '__main__':
    unittest.main()
