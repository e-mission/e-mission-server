# Standard imports
import unittest
import json
import logging
from datetime import datetime, timedelta

# Our imports
from emission.core.get_database import get_db, get_mode_db, get_section_db
from emission.net.api import visualize

logging.basicConfig(level=logging.DEBUG)

class TestVisualize(unittest.TestCase):
  def setUp(self):
    import emission.tests.common
    from copy import copy

    self.testUsers = ["test@example.com", "best@example.com", "fest@example.com",
                      "rest@example.com", "nest@example.com"]
    self.serverName = 'localhost'

    # Sometimes, we may have entries left behind in the database if one of the tests failed
    # or threw an exception, so let us start by cleaning up all entries
    emission.tests.common.dropAllCollections(get_db())
    self.ModesColl = get_mode_db()
    self.assertEquals(self.ModesColl.find().count(), 0)

    emission.tests.common.loadTable(self.serverName, "Stage_Modes", "emission/tests/data/modes.json")
    emission.tests.common.loadTable(self.serverName, "Stage_Sections", "emission/tests/data/testCarbonFile")
    self.SectionsColl = get_section_db()

    self.walkExpect = 1057.2524056424411
    self.busExpect = 2162.668467546699
    self.busCarbon = 267.0/1609
    self.airCarbon = 217.0/1609
    self.driveCarbon = 278.0/1609
    self.busOptimalCarbon = 92.0/1609

    self.now = datetime.now()
    self.dayago = self.now - timedelta(days=1)
    self.weekago = self.now - timedelta(weeks = 1)

    for section in self.SectionsColl.find():
      section['section_start_datetime'] = self.dayago
      section['section_end_datetime'] = self.dayago + timedelta(hours = 1)
      # Note that we currently only search by the date/time of the section, not the points.
      # If we ever change that, this test will start failing and will need to be fixed as well
      track_pt_array = []
      for i in range(10):
          track_pt_array.append({'time': '20140829T170451-0700',
                 'track_location': {'coordinates': [-122.114642519, 37.4021455446],
                                            'type': 'Point'}})
          track_pt_array.append({'time': '20140829T170620-0700',
                   'track_location': {'coordinates': [-122.1099155383, 37.399523614],
                    'type': 'Point'}})
      section['track_points'] = track_pt_array
      self.SectionsColl.save(section)

  def testCommutePopRoute(self):
    points = visualize.Commute_pop_route(5, self.weekago, self.now)
    # There are 5 sections with mode = 5 in the test data.
    # We add 20 points to each section
    # Then, we strip out the first 5 and last 5 points
    # So each section contributes 10 points to this
    # So we expect a total of 50 points
    self.assertEquals(len(points['latlng']), 50)

if __name__ == '__main__':
    unittest.main()
