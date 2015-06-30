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

  def testGetTwoSetsOfUserDataForPhone(self):
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

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    unittest.main()
