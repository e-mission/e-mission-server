import unittest
import mock
import json
from utils import load_database_json, purge_database_json
from main import Profile
from pymongo import MongoClient
import logging
from get_database import get_db, get_mode_db, get_section_db, get_trip_db, get_profile_db
import re
# Needed to modify the pythonpath
import sys
import os
from datetime import datetime, timedelta
from dao.user import User
from dao.client import Client
import tests.common

logging.basicConfig(level=logging.DEBUG)

class TestProfile(unittest.TestCase):
  def setUp(self):
    self.testUsers = ["test@example.com", "best@example.com", "fest@example.com",
                      "rest@example.com", "nest@example.com"]
    self.serverName = 'localhost'

    # Sometimes, we may have entries left behind in the database if one of the tests failed
    # or threw an exception, so let us start by cleaning up all entries
    tests.common.dropAllCollections(get_db())
    self.Profiles = get_profile_db()
    self.assertEquals(self.Profiles.find().count(), 0)
    load_database_json.loadTable(self.serverName, "Stage_Profiles", "tests/data/profiles.json")
    self.assertEquals(self.Profiles.find().count(), 1)
    # Let's make sure that the users are registered so that they have profiles
    for userEmail in self.testUsers:
      User.register(userEmail)

    self.walkExpect = 1057.2524056424411
    self.busExpect = 2162.668467546699
    self.busCarbon = 267.0/1609

    self.now = datetime.now()
    self.dayago = self.now - timedelta(days=1)
    self.weekago = self.now - timedelta(weeks = 1)

  def tearDown(self):
    for testUser in self.testUsers:
      purge_database_json.purgeData('localhost', testUser)
    self.Profiles.remove()
    self.assertEquals(self.Profiles.find().count(), 0)

  def testZipCreation(self):
    # Add some old sections that shouldn't be returned by the query
    # This one is just over a week old
    print self.Profiles
    prof_1 = self.Profiles.find_one({'user_id':'1'})
    self.assertEquals(prof_1['_id'],  '1')
    #zip creation phase, should make API call and obtain zipcode
    with mock.patch('main.Profile.detect_home', return_value=(-122.259769,37.871758)) as func1:
        with mock.patch('main.Profile.detect_home_from_db', return_value=(-122.259769,37.871758)) as func2:
            Profile.update_profiles(True)
            self.assertEquals(func1.called, True)
            self.assertEquals(func2.called, True)
            func1.reset_mock()
            func2.reset_mock()
            prof_1 = self.Profiles.find_one({'user_id':'1'})
            self.assertEquals(prof_1['zip'],  '94709')
            #zip should be stored, no API call
            Profile.update_profiles(True)
            prof_1 = self.Profiles.find_one({'user_id':'1'})
            self.assertEquals(prof_1['zip'],  '94709')
    #test new address/zipcode: database value does not equal home value
    with mock.patch('main.Profile.detect_home', return_value=(-122.2584,37.8697)) as func1:
        #with mock.patch('main.Profile.detect_home_from_db', return_value=(-122.259769,37.871758)) as func2:
            #new zip retrieval phase, should make API call and obtain zipcode
            Profile.update_profiles(True)
            self.assertEquals(func1.called, True)
            func1.reset_mock()
            prof_1 = self.Profiles.find_one({'user_id':'1'})
            self.assertEquals(prof_1['zip'],  '94720')
            #zip should be stored, no API call
            Profile.update_profiles(True)
            prof_1 = self.Profiles.find_one({'user_id':'1'})
            self.assertEquals(prof_1['zip'],  '94720')

if __name__ == '__main__':
    unittest.main()
