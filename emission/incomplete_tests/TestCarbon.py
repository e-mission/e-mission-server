from __future__ import print_function
from __future__ import division
from __future__ import unicode_literals
from __future__ import absolute_import
# Standard imports
from future import standard_library
standard_library.install_aliases()
from builtins import *
from past.utils import old_div
import unittest
import json
import logging
import re
from datetime import datetime, timedelta

# Our imports
from emission.analysis.result import carbon
import emission.core.get_database as edb
from emission.core.get_database import get_mode_db, get_section_db
import emission.tests.common as etc
from emission.core import common

class TestCarbon(unittest.TestCase):
  def setUp(self):
    from copy import copy

    self.testUsers = ["test@example.com", "best@example.com", "fest@example.com",
                      "rest@example.com", "nest@example.com"]
    self.serverName = 'localhost'

    # Sometimes, we may have entries left behind in the database if one of the tests failed
    # or threw an exception, so let us start by cleaning up all entries
    etc.dropAllCollections(edb._get_current_db())
    self.ModesColl = get_mode_db()
    self.assertEquals(self.ModesColl.estimated_document_count(), 0)

    etc.loadTable(self.serverName, "Stage_Modes", "emission/tests/data/modes.json")
    etc.loadTable(self.serverName, "Stage_Sections", "emission/tests/data/testCarbonFile")
    self.SectionsColl = get_section_db()

    self.walkExpect = 1057.2524056424411
    self.busExpect = 2162.668467546699
    self.busCarbon = old_div(267.0,1609)
    self.airCarbon = old_div(217.0,1609)
    self.driveCarbon = old_div(278.0,1609)
    self.busOptimalCarbon = old_div(92.0,1609)

    self.now = datetime.now()
    self.dayago = self.now - timedelta(days=1)
    self.weekago = self.now - timedelta(weeks = 1)

    for section in self.SectionsColl.find():
      section['section_start_datetime'] = self.dayago
      section['section_end_datetime'] = self.dayago + timedelta(hours = 1)
      if section['confirmed_mode'] == 5:
        airSection = copy(section)
        airSection['confirmed_mode'] = 9
        airSection['_id'] = section['_id'] + "_air"
        self.SectionsColl.insert(airSection)
          
      # print("Section start = %s, section end = %s" %
      #   (section['section_start_datetime'], section['section_end_datetime']))
      self.SectionsColl.save(section)

  def tearDown(self):
    for testUser in self.testUsers:
      etc.purgeSectionData(self.SectionsColl, testUser)
    self.ModesColl.remove()
    self.assertEquals(self.ModesColl.estimated_document_count(), 0)

  def getMyQuerySpec(self, user, modeId):
    return common.getQuerySpec(user, modeId, self.weekago, self.now)

  def testGetModes(self):
    modes = carbon.getAllModes()
    for mode in modes:
      print(mode['mode_id'], mode['mode_name'])
    self.assertEquals(len(modes), 9)

  def testGetDisplayModes(self):
    modes = carbon.getDisplayModes()
    for mode in modes:
      print(mode['mode_id'], mode['mode_name'])
    # skipping transport, underground and not a trip
    self.assertEquals(len(modes), 8)

  def testGetTripCountForMode(self):
    modes = carbon.getDisplayModes()
    # try different modes
    self.assertEqual(carbon.getTripCountForMode("test@example.com", 1, self.weekago, self.now), 1) # walk
    self.assertEqual(carbon.getTripCountForMode("test@example.com", 5, self.weekago, self.now), 1) # bus
    self.assertEqual(carbon.getTripCountForMode("test@example.com", 9, self.weekago, self.now), 1) # bus

    # try different users
    self.assertEqual(carbon.getTripCountForMode("best@example.com", 1, self.weekago, self.now), 1) # walk
    self.assertEqual(carbon.getTripCountForMode("rest@example.com", 5, self.weekago, self.now), 1) # bus

    # try to sum across users
    # We have 5 users - best, fest, rest, nest and test
    self.assertEqual(carbon.getTripCountForMode(None, 1, self.weekago, self.now), 5) # walk
    self.assertEqual(carbon.getTripCountForMode(None, 5, self.weekago, self.now), 5) # bus

  def testTotalModeShare(self):
    modeshare = carbon.getModeShare(None, self.weekago, self.now)
    self.assertEqual(modeshare['walking'], 5)
    self.assertEqual(modeshare['bus'], 5)
    self.assertEqual(modeshare['cycling'], 0)
    self.assertEqual(modeshare['car'], 0)
    self.assertEqual(modeshare['train'], 0)
    # self.assertFalse(modeshare.keys() contains 'not a trip')
    # self.assertFalse(modeshare.keys() contains 'transport')

  def testMyModeShare(self):
    modeshare = carbon.getModeShare('fest@example.com', self.weekago, self.now)
    print(modeshare)
    self.assertEqual(modeshare['walking'], 1)
    self.assertEqual(modeshare['bus'], 1)
    self.assertEqual(modeshare['cycling'], 0)
    self.assertEqual(modeshare['car'], 0)
    self.assertEqual(modeshare['train'], 0)
    # self.assertFalse(modeshare.keys() contains 'not a trip')
    # self.assertFalse(modeshare.keys() contains 'transport')

  def testDistanceForMode(self):
    # try different modes
    self.assertEqual(carbon.getDistanceForMode(self.getMyQuerySpec("test@example.com", 1)),
      self.walkExpect) # walk
    self.assertEqual(carbon.getDistanceForMode(self.getMyQuerySpec("test@example.com", 5)),
      self.busExpect) # bus

    # try different users
    self.assertEqual(carbon.getDistanceForMode(self.getMyQuerySpec("best@example.com", 1)), self.walkExpect) # walk
    self.assertEqual(carbon.getDistanceForMode(self.getMyQuerySpec("rest@example.com", 5)), self.busExpect) # bus

    # try to sum across users
    # We have 5 users - best, fest, rest, nest and test
    self.assertEqual(carbon.getDistanceForMode(self.getMyQuerySpec(None, 1)), len(self.testUsers) * self.walkExpect) # walk
    self.assertEqual(carbon.getDistanceForMode(self.getMyQuerySpec(None, 5)), len(self.testUsers) * self.busExpect) # bus

  def testMyModeDistance(self):
    myModeDistance = carbon.getModeShareDistance('fest@example.com', self.weekago, self.now)
    self.assertEqual(myModeDistance['walking'], self.walkExpect)
    self.assertEqual(myModeDistance['cycling'], 0)
    self.assertEqual(myModeDistance['bus'], self.busExpect)
    self.assertEqual(myModeDistance['train'], 0)

  def testTotalModeDistance(self):
    totalModeDistance = carbon.getModeShareDistance(None, self.weekago, self.now)
    self.assertEqual(totalModeDistance['walking'], len(self.testUsers) * self.walkExpect)
    self.assertEqual(totalModeDistance['cycling'], 0)
    self.assertEqual(totalModeDistance['bus'], len(self.testUsers) * self.busExpect)
    self.assertEqual(totalModeDistance['train'], 0)

  def testMyCarbonFootprint(self):
    myModeDistance = carbon.getModeCarbonFootprint('fest@example.com', carbon.carbonFootprintForMode, self.weekago, self.now)
    self.assertEqual(myModeDistance['walking'], 0)
    self.assertEqual(myModeDistance['cycling'], 0)
    self.assertEqual(myModeDistance['bus_short'], (self.busCarbon * self.busExpect/1000))
    self.assertEqual(myModeDistance['train_short'], 0)
    # We duplicate the bus trips to get air trips, so the distance should be the same
    self.assertEqual(myModeDistance['air_short'], (self.airCarbon * self.busExpect/1000))

  def testTotalCarbonFootprint(self):
    totalModeDistance = carbon.getModeCarbonFootprint(None, carbon.carbonFootprintForMode, self.weekago, self.now)
    self.assertEqual(totalModeDistance['walking'], 0)
    self.assertEqual(totalModeDistance['cycling'], 0)
    # We divide by 1000 to make it comprehensible in getModeCarbonFootprint
    self.assertEqual(totalModeDistance['bus_short'], old_div((self.busCarbon * len(self.testUsers) * self.busExpect),1000))
    self.assertEqual(totalModeDistance['air_short'], old_div((self.airCarbon * len(self.testUsers) * self.busExpect),1000))
    self.assertEqual(totalModeDistance['train_short'], 0)

  def testMySummary(self):
      (myModeShareCount, avgModeShareCount,
       myModeShareDistance, avgModeShareDistance,
       myModeCarbonFootprint, avgModeCarbonFootprint,
       myModeCarbonFootprintNoLongMotorized, avgModeCarbonFootprintNoLongMotorized,
       myOptimalCarbonFootprint, avgOptimalCarbonFootprint,
       myOptimalCarbonFootprintNoLongMotorized, avgOptimalCarbonFootprintNoLongMotorized) = carbon.getFootprintCompare('fest@example.com')

      # >>> m = {'air_long': 0, 'air_short': 0.2, 'bus_long': 0, 'bus_short': 0.3}
      # >>> f = [(i, m[i]) for i in m if m[i] != 0]
      # >>> f
      # [('bus_short', 0.3), ('air_short', 0.2)]
      # >>> dict(f)
      # {'bus_short': 0.3, 'air_short': 0.2}
      filterZero = lambda m: dict([(i, m[i]) for i in m if m[i] != 0])

      self.assertEqual(len(myModeShareCount), len(carbon.getDisplayModes()))
      self.assertEqual(len(myModeShareDistance), len(carbon.getDisplayModes()))
      # We have duplicated the bus trip to get bus, air and unconfirmed trips.
      # we ignore the unconfirmed trip, so only expect to get three values...
      self.assertAlmostEqual(sum(myModeShareDistance.values()), 2 * self.busExpect + self.walkExpect, places = 4)
      self.assertEqual(filterZero(myModeShareDistance),
                        {'bus': self.busExpect,
                         'walking': self.walkExpect,
                         'air': self.busExpect})
      logging.debug(filterZero(myModeShareDistance))
      self.assertEqual(filterZero(myModeCarbonFootprint),
            {'bus_short': old_div((self.busExpect * self.busCarbon),1000),
             'air_short': old_div((self.busExpect * self.airCarbon),1000)})
      self.assertEqual(filterZero(myModeCarbonFootprintNoLongMotorized),
            {'bus_short': old_div((self.busExpect * self.busCarbon),1000)})
      self.assertEqual(filterZero(myOptimalCarbonFootprint),
            {'air_short': old_div((self.busExpect * self.busOptimalCarbon),1000)})
      self.assertEqual(filterZero(myOptimalCarbonFootprintNoLongMotorized),
            {})

  def testSummaryAllTrips(self):
    summary = carbon.getSummaryAllTrips(self.weekago, self.now)
    # *2 because the walking trips don't count, but we have doubled the bus
    # trips to count as air trips
    self.assertEqual(summary['current'], old_div((self.busCarbon * self.busExpect + self.airCarbon * self.busExpect),1000))
    # No * 2 because the optimal value for short bus trips is to actually move to bikes :)
    self.assertEqual(summary['optimal'], old_div((self.busOptimalCarbon * self.busExpect),1000))
    # These are are without air, so will only count the bus trips
    self.assertEqual(summary['current no air'], old_div((self.busCarbon * self.busExpect),1000))
    self.assertEqual(summary['optimal no air'], 0)
    self.assertAlmostEqual(summary['all drive'], old_div((self.driveCarbon * (self.busExpect * 2 + self.walkExpect)),1000), places = 4)

  def testDistinctUserCount(self):
    self.assertEqual(carbon.getDistinctUserCount({}), len(self.testUsers))

  def testFilteredDistinctUserCount(self):
    # Now, move all the sections before a week
    # Now there should be no matches in the last week
    for section in self.SectionsColl.find():
      section['section_start_datetime'] = self.weekago + timedelta(days = -1)
      section['section_end_datetime'] = self.weekago + timedelta(days = -1) + timedelta(hours = 1)
      # print("Section start = %s, section end = %s" %
      #   (section['section_start_datetime'], section['section_end_datetime']))
      self.SectionsColl.save(section)
    print("About to check for distinct users from a week ago")
    self.assertEqual(carbon.getDistinctUserCount(carbon.getQuerySpec(None, None,
                                                 self.weekago, self.now)), 0)
    self.assertEqual(carbon.getDistinctUserCount(carbon.getQuerySpec(None, None,
                     self.weekago + timedelta(weeks = -1), self.now)), len(self.testUsers))

  def testDelLongMotorizedModes(self):
    testMap = {'bus': 1, 'air': 3}
    carbon.delLongMotorizedModes(testMap)
    self.assertEqual(len(testMap), 1)
    self.assertEqual(testMap, {'bus': 1})

  def testDelLongMotorizedModesShortLong(self):
    testMap = {'bus_short': 1, 'bus_long': 2, 'air_short': 3, 'air_long': 4}
    carbon.delLongMotorizedModes(testMap)
    self.assertEqual(len(testMap), 2)
    self.assertIn('bus_short', testMap)
    self.assertIn('bus_long', testMap)
    self.assertNotIn('air_short', testMap)
    self.assertNotIn('air_long', testMap)

  def testGetCarbonFootprintsForMap(self):
    testDistanceMap = {'a': 1, 'b': 2, 'c': 3}
    testModeFootprintMap = {'a': 1, 'b': 2, 'c': 3}

    footprintMap = carbon.getCarbonFootprintsForMap(testDistanceMap, testModeFootprintMap)
    self.assertEqual(footprintMap, {'a': 0.001, 'b': 0.004, 'c': 0.009})

  def testAvgCalculation(self):
    testMap = {'a': 5, 'b': 10, 'c': 15, 'd': 3, 'e': 7, 'f': 13}
    avgTestMap = carbon.convertToAvg(testMap, 5)
    self.assertEquals(avgTestMap['a'], 1)
    self.assertEquals(avgTestMap['b'], 2)
    self.assertEquals(avgTestMap['c'], 3)
    self.assertEquals(avgTestMap['d'], 0.6)
    self.assertEquals(avgTestMap['e'], 1.4)
    self.assertEquals(avgTestMap['f'], 2.6)

if __name__ == '__main__':
    etc.configLogging()
    unittest.main()
