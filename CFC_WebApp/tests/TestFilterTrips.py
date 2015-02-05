import unittest
import json
from utils import load_database_json, purge_database_json
from main import tripManager
from pymongo import MongoClient
import logging
from get_database import get_db, get_mode_db, get_section_db, get_trip_db
import re
# Needed to modify the pythonpath
import sys
import os
from datetime import datetime, timedelta
from dao.user import User
from dao.client import Client
import tests.common

logging.basicConfig(level=logging.DEBUG)

class TestFilterTrips(unittest.TestCase):
  def setUp(self):
    self.testUsers = ["test@example.com", "best@example.com", "fest@example.com",
                      "rest@example.com", "nest@example.com"]
    self.serverName = 'localhost'

    # Sometimes, we may have entries left behind in the database if one of the tests failed
    # or threw an exception, so let us start by cleaning up all entries
    tests.common.dropAllCollections(get_db())
    self.ModesColl = get_mode_db()
    # self.ModesColl.remove()
    self.assertEquals(self.ModesColl.find().count(), 0)

    self.SectionsColl = get_section_db()
    # self.SectionsColl.remove()
    self.assertEquals(self.SectionsColl.find().count(), 0)

    load_database_json.loadTable(self.serverName, "Stage_Modes", "tests/data/modes.json")
    load_database_json.loadTable(self.serverName, "Stage_Sections", "tests/data/testFilterFile")

    # Let's make sure that the users are registered so that they have profiles
    for userEmail in self.testUsers:
      User.register(userEmail)

    self.walkExpect = 1057.2524056424411
    self.busExpect = 2162.668467546699
    self.busCarbon = 267.0/1609

    self.now = datetime.now()
    self.dayago = self.now - timedelta(days=1)
    self.weekago = self.now - timedelta(weeks = 1)

    for section in self.SectionsColl.find():
      section['section_start_datetime'] = self.dayago
      section['section_end_datetime'] = self.dayago + timedelta(hours = 1)
      section['section_start_point'] = "filler start point"
      section['section_end_point'] = "filler end point"
      section['predicted_mode'] = [0, 0.4, 0.6, 0]
      section['confirmed_mode'] = ''
      # print("Section start = %s, section end = %s" %
      #   (section['section_start_datetime'], section['section_end_datetime']))
      # Replace the user email with the UUID
      section['user_id'] = User.fromEmail(section['user_id']).uuid
      self.SectionsColl.save(section)

  def tearDown(self):
    for testUser in self.testUsers:
      purge_database_json.purgeData('localhost', testUser)
    self.ModesColl.remove()
    self.assertEquals(self.ModesColl.find().count(), 0)

  def testGetUnclassifiedSectionsFiltered(self):
    """
    Tests that queryUnclassifiedSections never returns 
    a section with section['filter'] == True. A section is only returned if 
    section['filter'] == False and all other query conditions are met    
    """
    from dao.user import User

    fakeEmail = "fest@example.com"

    client = Client("testclient")
    client.update(createKey = False)
    tests.common.makeValid(client)

    (resultPre, resultReg) = client.preRegister("this_is_the_super_secret_id", fakeEmail)
    self.assertEqual(resultPre, 0)
    self.assertEqual(resultReg, 1)

    user = User.fromEmail(fakeEmail)
    self.assertEqual(user.getFirstStudy(), 'testclient')

    unclassifiedSections = tripManager.getUnclassifiedSections(User.fromEmail(fakeEmail).uuid)['sections']
    # Check that of the valid sections in the testFilterFile (2/3), only one of them is returned by the query
    self.assertEqual(len(unclassifiedSections), 1)
    # Check that the second entry in the testFilterFile is the only section 
    # that is loaded into the database
    self.assertEqual('20140401T095738-0700',unclassifiedSections[0]['trip_id'])                     

if __name__ == '__main__':
    unittest.main()
