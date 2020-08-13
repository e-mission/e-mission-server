from __future__ import division
from __future__ import unicode_literals
from __future__ import print_function
from __future__ import absolute_import
# Standard imports
from future import standard_library
standard_library.install_aliases()
from builtins import *
from past.utils import old_div
import unittest
import mock
import json
import logging
from datetime import datetime, timedelta
import re

# Our imports
from emission.net.api import Profile
import emission.core.get_database as edb
from emission.core.get_database import get_mode_db, get_section_db, get_trip_db, get_profile_db
from emission.core.wrapper.user import User
from emission.core.wrapper.client import Client
import emission.tests.common

class TestProfile(unittest.TestCase):
  def setUp(self):
    self.testUsers = ["test@example.com", "best@example.com", "fest@example.com",
                      "rest@example.com", "nest@example.com"]
    self.serverName = 'localhost'

    # Sometimes, we may have entries left behind in the database if one of the tests failed
    # or threw an exception, so let us start by cleaning up all entries
    emission.tests.common.dropAllCollections(edb._get_current_db())
    self.Profiles = get_profile_db()
    self.assertEquals(self.Profiles.estimated_document_count(), 0)
    emission.tests.common.loadTable(self.serverName, "Stage_Profiles", "emission/tests/data/profiles.json")
    self.assertEquals(self.Profiles.estimated_document_count(), 1)
    # Let's make sure that the users are registered so that they have profiles
    for userEmail in self.testUsers:
      User.register(userEmail)

    self.walkExpect = 1057.2524056424411
    self.busExpect = 2162.668467546699
    self.busCarbon = old_div(267.0,1609)

    self.now = datetime.now()
    self.dayago = self.now - timedelta(days=1)
    self.weekago = self.now - timedelta(weeks = 1)

  def tearDown(self):
    for testUser in self.testUsers:
      emission.tests.common.purgeSectionData(get_section_db(), testUser)
    self.Profiles.remove()
    self.assertEquals(self.Profiles.estimated_document_count(), 0)

  def testZipCreation(self):
    # Make sure that zipcodes are updated in the database, correctly and when needed
    prof_1 = self.Profiles.find_one({'user_id':'1'})
    self.assertEquals(prof_1['_id'],  '1')
    #zip creation phase, should make API call and obtain zipcode
    with mock.patch('emission.net.api.Profile.detect_home', return_value=(-122.259769,37.871758)) as func1:
        with mock.patch('emission.net.api.Profile.detect_home_from_db', return_value=(-122.259769,37.871758)) as func2:
            Profile.update_profiles(True)
            self.assertEquals(func1.called, True)
            self.assertEquals(func2.called, True)
            func1.reset_mock()
            func2.reset_mock()
            prof_1 = self.Profiles.find_one({'user_id':'1'})
            self.assertEquals(prof_1['zip'],  '94704')
    #test new address/zipcode: database value does not equal home value
    with mock.patch('emission.net.api.Profile.detect_home', return_value=(-122.2584,37.8697)) as func1:
            #new zip retrieval phase, should make API call and obtain zipcode
            Profile.update_profiles(True)
            self.assertEquals(func1.called, True)
            func1.reset_mock()
            prof_1 = self.Profiles.find_one({'user_id':'1'})
            self.assertEquals(prof_1['zip'],  '94720')

  def testZipAPI(self):
    # Check to make sure that the Geocoder API is not used if not needed
    # Only checks that geocode IS NOT called the second time (zipcode MUST come from somwhere, ensuring that Google API is called the first time)
    prof_1 = self.Profiles.find_one({'user_id':'1'})
    self.assertEquals(prof_1['_id'],  '1')
    #zip creation phase, should make API call and obtain zipcode
    with mock.patch('emission.net.api.Profile.detect_home', return_value=(-122.259769,37.871758)) as func1:
        with mock.patch('emission.net.api.Profile.detect_home_from_db', return_value=(-122.259769,37.871758)) as func2:
            Profile.update_profiles(True)
            self.assertEquals(func1.called, True)
            self.assertEquals(func2.called, True)
            func1.reset_mock()
            func2.reset_mock()
            prof_1 = self.Profiles.find_one({'user_id':'1'})
            self.assertEquals(prof_1['zip'],  '94704')
            #zip should be stored, no API call
            with mock.patch('emission.net.api.Profile.Geocoder.reverse_geocode', return_value=None) as func3:
                Profile.update_profiles(True)
                self.assertEquals(func3.called, False)
                prof_1 = self.Profiles.find_one({'user_id':'1'})
                self.assertEquals(prof_1['zip'],  '94704')
    with mock.patch('emission.net.api.Profile.detect_home', return_value=(-122.2584,37.8697)) as func1:
            #new zip retrieval phase, should make API call and obtain zipcode
                Profile.update_profiles(True)
                prof_1 = self.Profiles.find_one({'user_id':'1'})
                self.assertEquals(prof_1['zip'],  '94720')
                with mock.patch('emission.net.api.Profile.Geocoder.reverse_geocode', return_value=None) as func3:
                    #zip should be stored, no API call
                    Profile.update_profiles(True)
                    self.assertEquals(func3.called, False)
                    func3.reset_mock()
                    prof_1 = self.Profiles.find_one({'user_id':'1'})
                    self.assertEquals(prof_1['zip'],  '94720')

if __name__ == '__main__':
    import emission.tests.common as etc
    etc.configLogging()

    unittest.main()
