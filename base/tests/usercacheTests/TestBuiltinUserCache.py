import unittest
import json
import sys
import os
import uuid
import logging

import tests.common

# Technically, unit tests should only test one piece of functionality
# I am breaking that rule here to test both the put and the get at the same time, because:
# a) we get to ensure that the put and get are consistent
# b) otherwise, we would have to replicate the "get" code here

import usercache.abstract_usercache as ucauc # ucauc = usercache.abstract_usercache
import main.usercache as mauc
from get_database import get_db, get_usercache_db

class TestBuiltinUserCache(unittest.TestCase):
  def setUp(self):
    self.testUserUUID = uuid.uuid4()
    tests.common.dropAllCollections(get_db())

  def tearDown(self):
    tests.common.dropAllCollections(get_db())

  def testPutUserDataForPhone(self):
    uc = ucauc.UserCache.getUserCache(self.testUserUUID)
    footprintData = {"footprint": {"mine": 30, "avg": 40, "optimal": 50, "alldrive": 60}}
    uc.putUserDataForPhone("data", footprintData)

    retrievedData = mauc.sync_server_to_phone(self.testUserUUID)
    logging.debug("retrievedData = %s" % retrievedData)
    self.assertTrue(retrievedData is not None) # if it doesn't exist, the method returns None
    self.assertTrue("user" in retrievedData)
    self.assertTrue("data" in retrievedData["user"])
    self.assertTrue("footprint" in retrievedData["user"]["data"])
    self.assertTrue("game" not in retrievedData["user"]["data"])

  def testPutTwoSetsOfUserDataForPhone(self):
    uc = ucauc.UserCache.getUserCache(self.testUserUUID)

    footprintData = {"footprint": {"mine": 30, "avg": 40, "optimal": 50, "alldrive": 60}}
    uc.putUserDataForPhone("data", footprintData)

    gameData = {"my_score": 30, "other_scores": {'josh': 40, 'jillie': 20, 'naomi': 50}}
    uc.putUserDataForPhone("game", gameData)

    retrievedData = mauc.sync_server_to_phone(self.testUserUUID)
    logging.debug("retrievedData = %s" % retrievedData)
    self.assertTrue(retrievedData is not None) # if it doesn't exist, the method returns None

    self.assertTrue("user" in retrievedData)
    self.assertTrue("data" in retrievedData["user"])
    self.assertTrue("footprint" in retrievedData["user"]["data"])
    self.assertTrue("game" in retrievedData["user"])
    self.assertTrue("my_score" in retrievedData["user"]["game"])
    self.assertTrue("other_scores" in retrievedData["user"]["game"])

  def testGetTwoSetsOfUserDataFromPhone(self):
    user_data_from_phone = {
        'user': {
            'mode_confirmations': {
                "section_1": "walking",
                "section_2": "cycling",
                "section_3": "driving"
            },
            'deleted_sections':
                ['section_4', 'section_5']
        }
    }

    mauc.sync_phone_to_server(self.testUserUUID, user_data_from_phone)

    uc = ucauc.UserCache.getUserCache(self.testUserUUID)
    self.assertEqual(uc.getUserDataFromPhone("deleted_sections"), ['section_4', 'section_5'])
    self.assertEqual(len(uc.getUserDataFromPhone("mode_confirmations")), 3)
    self.assertEqual(uc.getUserDataFromPhone("mode_confirmations")['section_1'], "walking")

  def testPutTwoSetsOfBackgroundConfigForPhone(self):
    uc = ucauc.UserCache.getUserCache(self.testUserUUID)

    pull_probes_list = ["accelerometer", "gyrometer", "linear_accelerometer"]
    uc.putBackgroundConfigForPhone("pull_probes", pull_probes_list)

    location_config = {"accuracy": "POWER_BALANCED_ACCURACY",
                       "filter": "DISTANCE_FILTER",
                       "geofence_radius": 100,
                      }
    uc.putBackgroundConfigForPhone("location", location_config)

    retrievedData = mauc.sync_server_to_phone(self.testUserUUID)
    logging.debug("retrievedData = %s" % retrievedData)
    self.assertTrue(retrievedData is not None) # if it doesn't exist, the method returns None

    expectedData = {
      "background_config": {
          "pull_probes": ["accelerometer", "gyrometer", "linear_accelerometer"],
          "location": {
            "accuracy": "POWER_BALANCED_ACCURACY",
            "filter": "DISTANCE_FILTER",
            "geofence_radius": 100,
          }
      }
    }

    self.assertEqual(retrievedData, expectedData)

  def testGetTwoSetsOfBackgroundDataFromPhone(self):
    background_data_from_phone = {
        'background': {
            'locations': [
              {'mLat': 34.25, 'mLng': -122.45, 'time': 12345, 'elapsed_time': 2345, 'accuracy': 25},
              {'mLat': 35.25, 'mLng': -123.45, 'time': 12355, 'elapsed_time': 2355, 'accuracy': 26},
            ],
            'activities': [
              {"mode": "cycling", "confidence": 20, "time": 22334},
              {"mode": "walking", "confidence": 20, "time": 33445},
            ],
            'accelerometer': [
              {"time": 11223, "x": 123.4, "y": 234.5, "z": 345.6},
              {"time": 11224, "x": 123.4, "y": 234.5, "z": 345.6}
            ],
        }
    }

    mauc.sync_phone_to_server(self.testUserUUID, background_data_from_phone)

    uc = ucauc.UserCache.getUserCache(self.testUserUUID)
    self.assertEqual(len(uc.getBackgroundDataFromPhone("locations")), 2)
    self.assertEqual(len(uc.getBackgroundDataFromPhone("activities")), 2)
    self.assertEqual(len(uc.getBackgroundDataFromPhone("accelerometer")), 2)
    self.assertEqual(uc.getBackgroundDataFromPhone("locations")[1]["time"], 12355)
    self.assertEqual(uc.getBackgroundDataFromPhone("activities")[1]["time"], 33445)
    self.assertEqual(uc.getBackgroundDataFromPhone("accelerometer")[1]["time"], 11224)

  def testClearBackgroundData(self):
    background_data_from_phone = {
        'background': {
            'locations': [
              {'mLat': 34.25, 'mLng': -122.45, 'time': 12345, 'elapsed_time': 2345, 'accuracy': 25},
              {'mLat': 35.25, 'mLng': -123.45, 'time': 12355, 'elapsed_time': 2355, 'accuracy': 26},
            ],
            'activities': [
              {"mode": "cycling", "confidence": 20, "time": 22334},
              {"mode": "walking", "confidence": 20, "time": 33445},
            ],
            'accelerometer': [
              {"time": 11223, "x": 123.4, "y": 234.5, "z": 345.6},
              {"time": 11224, "x": 123.4, "y": 234.5, "z": 345.6}
            ],
        }
    }

    mauc.sync_phone_to_server(self.testUserUUID, background_data_from_phone)

    uc = ucauc.UserCache.getUserCache(self.testUserUUID)
    self.assertEqual(len(uc.getBackgroundDataFromPhone("locations")), 2)
    self.assertEqual(len(uc.getBackgroundDataFromPhone("activities")), 2)
    self.assertEqual(len(uc.getBackgroundDataFromPhone("accelerometer")), 2)
  
    uc.clearBackgroundDataFromPhone(["locations", "accelerometer"])
    self.assertEqual(uc.getBackgroundDataFromPhone("locations"), None)
    self.assertEqual(len(uc.getBackgroundDataFromPhone("activities")), 2)
    self.assertEqual(uc.getBackgroundDataFromPhone("accelerometer"), None)

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    unittest.main()
