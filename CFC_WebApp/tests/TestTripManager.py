import unittest
import json
from utils import load_database_json
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

class TestTripManager(unittest.TestCase):
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
    load_database_json.loadTable(self.serverName, "Stage_Sections", "tests/data/testCarbonFile")

    # Let's make sure that the users are registered so that they have profiles
    for userEmail in self.testUsers:
      User.register(userEmail)

    self.walkExpect = 1057.2524056424411
    self.busExpect = 2162.668467546699
    self.busCarbon = 267.0/1609

    self.now = datetime.now()
    self.dayago = self.now - timedelta(days=1)
    self.weekago = self.now - timedelta(weeks = 1)
    tests.common.updateSections(self)

  def tearDown(self):
    for testUser in self.testUsers:
      tests.common.purgeSectionData(get_section_db(), testUser)
    self.ModesColl.remove()
    self.assertEquals(self.ModesColl.find().count(), 0)

  def testQueryUnclassifiedSectionsWeekAgo(self):
    # Add some old sections that shouldn't be returned by the query
    # This one is just over a week old
    old_sec_1 = self.SectionsColl.find_one({'$and': [{'user_id': User.fromEmail('fest@example.com').uuid}, {'type':'move'}, {'mode':1}]})
    old_sec_1['_id'] = 'old_sec_1'
    old_sec_1['section_start_datetime'] = self.weekago - timedelta(minutes = 30)
    old_sec_1['section_end_datetime'] = self.weekago - timedelta(minutes = 5)
    logging.debug("Inserting old_sec_1 %s" % old_sec_1)
    self.SectionsColl.insert(old_sec_1)

    # This one is a month old
    monthago = self.now - timedelta(days = 30)
    old_sec_2 = self.SectionsColl.find_one({'$and': [{'user_id':User.fromEmail('fest@example.com').uuid}, {'type':'move'}, {'mode':4}]})
    old_sec_2['_id'] = 'old_sec_2'
    old_sec_2['section_start_datetime'] = monthago - timedelta(minutes = 30)
    old_sec_2['section_end_datetime'] = monthago - timedelta(minutes = 5)
    logging.debug("Inserting old_sec_2 %s" % old_sec_2)
    self.SectionsColl.insert(old_sec_2)

    # This one is missing the predicted mode
    monthago = self.now - timedelta(days = 30)
    un_pred_sec = self.SectionsColl.find_one({'$and': [{'user_id':User.fromEmail('fest@example.com').uuid}, {'type':'move'}, {'mode':4}]})
    un_pred_sec['_id'] = 'un_pred_sec'
    del un_pred_sec['predicted_mode']
    logging.debug("Inserting un_pred_sec %s" % un_pred_sec)
    self.SectionsColl.insert(un_pred_sec)

    queriedUnclassifiedSections = tripManager.queryUnclassifiedSections(User.fromEmail('fest@example.com').uuid)
    self.assertEqual(queriedUnclassifiedSections.count(), 2)

  def testQueryUnclassifiedSectionsLowConfidence(self):
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

    queriedUnclassifiedSections = tripManager.queryUnclassifiedSections(User.fromEmail(fakeEmail).uuid)
    self.assertEqual(queriedUnclassifiedSections.count(), 2)

    # Set the auto_confirmed values for the trips
    for section in queriedUnclassifiedSections:
      print section['_id']
      self.SectionsColl.update({'_id': section['_id']}, {'test_auto_confirmed': {'mode': section['mode'], 'prob': 0.95}})

    # Now, set the update timestamp to two weeks ago so that we will start filtering
    tests.common.updateUserCreateTime(user.uuid)
    queriedUnclassifiedSections = tripManager.queryUnclassifiedSections(User.fromEmail(fakeEmail).uuid)
    self.assertEqual(queriedUnclassifiedSections.count(), 0)

  def testStoreSensedTrips(self):
    fakeEmail = "fest@example.com"
    fakeUUID = User.fromEmail(fakeEmail).uuid

    trip_array = json.load(open("tests/data/sensed_trips.json"))
    self.assertEqual(len(trip_array), 2)
    tripManager.storeSensedTrips(fakeUUID, trip_array)
    insertedTrips = [trip for trip in get_trip_db().find({"user_id": fakeUUID})]
    # We load two sections for each user in the setup. Here we only want to
    # look at sections that we added here. We distinguish between the two by looking
    # to see whether the predicted mode exists
    insertedSections = [section for section in get_section_db().find({"$and":
        [{"user_id": fakeUUID}, {"predicted_mode": {"$exists": False}}]})]
    # insertedSections = [section["predicted_mode"] for section in get_section_db().find({"user_id": fakeUUID})]

    self.assertEqual(len(insertedTrips), 2)

    self.assertEqual(insertedTrips[0]["type"], "place")
    self.assertEqual(insertedTrips[0]["trip_start_time"], "20150101T000153-0500")
    # self.assertEqual(insertedTrips[0]["trip_start_datetime"], datetime(2014,12,31,17,31,52))
    self.assertEqual(insertedTrips[0]["trip_end_time"], "20150102T000252-0500")
    # self.assertEqual(insertedTrips[0]["trip_end_datetime"], datetime(2015,01,02,04,01,51))

    startPlaceLocation = insertedTrips[0]["place"]["place_location"]
    self.assertEqual(startPlaceLocation["coordinates"], [-122.086945, 37.380866])

    self.assertEqual(insertedTrips[1]["type"], "move")
    self.assertEqual(insertedTrips[1]["trip_start_time"], "20150102T000252-0500")
    self.assertEqual(insertedTrips[1]["trip_end_time"], "20150102T000252-0500")

    self.assertEqual(len(insertedSections), 2)
    walkingSection = insertedSections[0]
    walkingTrackPointArray = insertedSections[0]["track_points"]

    self.assertEqual(walkingSection["section_start_time"], "20150102T000252-0500")
    self.assertEqual(walkingSection["section_end_time"], "20150102T000253-0500")
    self.assertEqual(walkingSection["duration"], 180631)
    self.assertAlmostEqual(walkingSection["distance"], 1311.125, places=2)

    self.assertEqual(len(walkingTrackPointArray), 7)
    self.assertEqual(walkingTrackPointArray[0]["track_location"]["coordinates"], [-122.086945, 37.380866])
    #self.assertEqual(walkingTrackPointArray[8]["track_location"]["coordinates"], [-122.078265, 37.385461])

  def testGetUnclassifiedSectionsFiltered(self):
    """
    Tests that queryUnclassifiedSections never returns 
    a section with section['retained'] == False. A section is only returned if 
    section['retained'] == True  and all other query conditions are met    
    """
    # Clear previous Stage_Sections data and load new data
    # specific to filtering
    self.SectionsColl.remove()
    load_database_json.loadTable(self.serverName, "Stage_Sections", "tests/data/testFilterFile")   
    tests.common.updateSections(self) 
    # Extra updates to Sections necessary for testing filtering
    for section in self.SectionsColl.find():
      section['section_start_point'] = "filler start point"
      section['section_end_point'] = "filler end point"   
      self.SectionsColl.save(section)

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
    self.assertEqual('20140401T095742-0700',unclassifiedSections[0]['trip_id'])                     


if __name__ == '__main__':
    unittest.main()
