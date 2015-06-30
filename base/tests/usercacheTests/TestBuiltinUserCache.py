import unittest
import json
import sys
import os
import uuid
import logging

import tests.common

import usercache.abstract_usercache as ucauc # ucauc = usercache.abstract_usercache
import usercache.builtin_usercache as biuc
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

    retrievedData = get_usercache_db().find_one(self.testUserUUID)
    logging.debug("retrievedData = %s" % retrievedData)

    self.assertTrue("server_to_phone" in retrievedData)
    self.assertTrue("user" in retrievedData["server_to_phone"])
    self.assertTrue("data" in retrievedData["server_to_phone"]["user"])
    self.assertTrue("footprint" in retrievedData["server_to_phone"]["user"]["data"])
    self.assertTrue("game" not in retrievedData["server_to_phone"]["user"]["data"])

  def testPutTwoSetsOfUserDataForPhone(self):
    uc = biuc.BuiltinUserCache(self.testUserUUID)
    footprintData = {"footprint": {"mine": 30, "avg": 40, "optimal": 50, "alldrive": 60}}
    uc.putUserDataForPhone("data", footprintData)

    gameData = {"my_score": 30, "other_scores": {'josh': 40, 'jillie': 20, 'naomi': 50}}
    uc.putUserDataForPhone("game", gameData)

    retrievedData = get_usercache_db().find_one(self.testUserUUID)
    logging.debug("retrievedData = %s" % retrievedData)

    self.assertTrue("server_to_phone" in retrievedData)
    self.assertTrue("user" in retrievedData["server_to_phone"])
    self.assertTrue("data" in retrievedData["server_to_phone"]["user"])
    self.assertTrue("footprint" in retrievedData["server_to_phone"]["user"]["data"])
    self.assertTrue("game" in retrievedData["server_to_phone"]["user"])
    self.assertTrue("my_score" in retrievedData["server_to_phone"]["user"]["game"])
    self.assertTrue("other_scores" in retrievedData["server_to_phone"]["user"]["game"])

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    unittest.main()
