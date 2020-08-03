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
import json
import sys
import os
import uuid
import logging
import time

# Our imports
import emission.tests.common

# Technically, unit tests should only test one piece of functionality
# I am breaking that rule here to test both the put and the get at the same time, because:
# a) we get to ensure that the put and get are consistent
# b) otherwise, we would have to replicate the "get" code here

import emission.net.usercache.abstract_usercache as ucauc # ucauc = usercache.abstract_usercache
import emission.storage.pipeline_queries as esp
import emission.storage.timeseries.timequery as estt
import emission.net.api.usercache as mauc
import emission.core.get_database as edb

class TestBuiltinUserCache(unittest.TestCase):
  def setUp(self):
    self.testUserUUID = uuid.uuid4()
    emission.tests.common.dropAllCollections(edb._get_current_db())

  def tearDown(self):
    emission.tests.common.dropAllCollections(edb._get_current_db())

  def testPutUserDataForPhone(self):
    uc = ucauc.UserCache.getUserCache(self.testUserUUID)
    footprintData = {"mine": 30, "avg": 40, "optimal": 50, "alldrive": 60}
    uc.putDocument("data/footprint", footprintData)

    retrievedData = mauc.sync_server_to_phone(self.testUserUUID)
    logging.debug("retrievedData = %s" % retrievedData)
    self.assertTrue(retrievedData is not None) # if it doesn't exist, the method returns None
    self.assertEqual(len(retrievedData),1)
    self.assertEqual(sorted(list(retrievedData[0].keys())), ["data", "metadata"])
    self.assertEqual(retrievedData[0]["data"]["mine"], 30)

  def testPutTwoSetsOfUserDataForPhone(self):
    uc = ucauc.UserCache.getUserCache(self.testUserUUID)

    footprintData = {"mine": 30, "avg": 40, "optimal": 50, "alldrive": 60}
    uc.putDocument("data/footprint", footprintData)

    gameData = {"my_score": 30, "other_scores": {'josh': 40, 'jillie': 20, 'naomi': 50}}
    uc.putDocument("data/game", gameData)

    retrievedData = mauc.sync_server_to_phone(self.testUserUUID)
    logging.debug("retrievedData = %s" % retrievedData)
    self.assertTrue(retrievedData is not None) # if it doesn't exist, the method returns None

    self.assertEqual(len(retrievedData), 2)
    self.assertTrue(list(retrievedData[0].keys()), ["data", "metadata"])
    self.assertTrue(list(retrievedData[1].keys()), ["data", "metadata"])

    for data in retrievedData:
        if data["metadata"]["key"] == "data/game":
            self.assertEqual(data["data"]["my_score"], 30)

  def testClearObsoleteDocument(self):
    self.testPutTwoSetsOfUserDataForPhone()

    uc = ucauc.UserCache.getUserCache(self.testUserUUID)
    uc.clearObsoleteDocument("data/footprint")
    uc.clearObsoleteDocument("data/game")

    retrievedData = mauc.sync_server_to_phone(self.testUserUUID)
    logging.debug("retrievedData = %s" % retrievedData)
    
    self.assertTrue(retrievedData is not None) # if it doesn't exist, the method returns None
    self.assertEqual(len(retrievedData), 0)

  def testGetDocumentKeyList(self):
    self.testPutTwoSetsOfUserDataForPhone()

    uc = ucauc.UserCache.getUserCache(self.testUserUUID)
    self.assertEqual(uc.getDocumentKeyList(), ["data/footprint", "data/game"])
    uc.clearObsoleteDocument("data/footprint")
    uc.clearObsoleteDocument("data/game")

    retrievedData = mauc.sync_server_to_phone(self.testUserUUID)
    logging.debug("retrievedData = %s" % retrievedData)
    
    self.assertTrue(retrievedData is not None) # if it doesn't exist, the method returns None
    self.assertEqual(len(retrievedData), 0)

  def testGetTwoSetsOfUserDataFromPhone(self):
    user_data_from_phone = [
      {
        "metadata": {
          "write_ts": 1435856237,
          "type": "rw-document",
          "key": "diary/mode-confirmation",
        },
        "data" : {
          "sid": "this-is-one-long-section-id",
          "mode": "walking",
        }
      },
      {
        "metadata": {
          "write_ts": 1435856337,
          "type": "rw-document",
          "key": "diary/mode-confirmation",
        },
        "data" : {
          "sid": "this-is-two-long-section-id",
          "mode": "cycling",
        }
      },
    ]

    mauc.sync_phone_to_server(self.testUserUUID, user_data_from_phone)

    uc = ucauc.UserCache.getUserCache(self.testUserUUID)
    msgs = uc.getMessage(key_list = ["diary/mode-confirmation"])
    self.assertEqual(len(msgs), 2)
    self.assertEqual(msgs[0]["data"]["mode"], "walking")

  def testPutTwoSetsOfBackgroundConfigForPhone(self):
    start_ts = time.time()
    uc = ucauc.UserCache.getUserCache(self.testUserUUID)

    pull_probes_list = ["accelerometer", "gyrometer", "linear_accelerometer"]
    uc.putDocument("config/pull_probes", pull_probes_list)

    location_config = {"accuracy": "POWER_BALANCED_ACCURACY",
                       "filter": "DISTANCE_FILTER",
                       "geofence_radius": 100,
                      }
    uc.putDocument("config/location_config", location_config)

    end_ts = time.time()

    retrievedData = mauc.sync_server_to_phone(self.testUserUUID)
    logging.debug("retrievedData = %s" % retrievedData)
    self.assertTrue(retrievedData is not None) # if it doesn't exist, the method returns None

    expectedData = [
        ["accelerometer", "gyrometer", "linear_accelerometer"],
        {
          "accuracy": "POWER_BALANCED_ACCURACY",
          "filter": "DISTANCE_FILTER",
          "geofence_radius": 100,
        }
    ]
    expectedKeys = ["config/pull_probes", "config/location_config"]

    for i, rd in enumerate(retrievedData):
        self.assertEqual(rd["data"], expectedData[i])

    for i, rd in enumerate(retrievedData):
        self.assertEqual(rd["metadata"]["key"], expectedKeys[i])

    for i, rd in enumerate(retrievedData):
        self.assertGreaterEqual(rd["metadata"]["write_ts"], start_ts) and \
            self.assertLessEqual(rd["metadata"]["write_ts"], end_ts)

  def testGetTwoSetsOfBackgroundDataFromPhone(self):
    background_data_from_phone = [
      {
        "metadata": {
          "write_ts": 1435856235,
          "type": "message",
          "key": "background/location",
        },
        "data" : { "mLat": 45.64, "mLng": 21.35, "mElapsedTime": 112233, }
      },
      {
        "metadata": {
          "write_ts": 1435886337,
          "type": "message",
          "key": "background/location",
        },
        "data" : { "mLat": 49.64, "mLng": 25.35, "mElapsedTime": 142233, }
      },
      {
        "metadata": {
          "write_ts": 1435856337,
          "type": "message",
          "key": "background/activity",
        },
        "data" : { "mode": "walking", "confidence": 90 }
      },
      {
        "metadata": {
          "write_ts": 1435886337,
          "type": "message",
          "key": "background/activity",
        },
        "data" : { "mode": "cycling", "confidence": 70 }
      },
      {
        "metadata": {
          "write_ts": 1435856337,
          "type": "message",
          "key": "background/accelerometer",
        },
        "data" : {"x": 1234, "y": 2345, "z": 3456}
      },
      {
        "metadata": {
          "write_ts": 1435886337,
          "type": "message",
          "key": "background/accelerometer",
        },
        "data" : {"x": 2345, "y": 3456, "z": 4567}
      },
    ]

    mauc.sync_phone_to_server(self.testUserUUID, background_data_from_phone)

    uc = ucauc.UserCache.getUserCache(self.testUserUUID)
    self.assertEqual(len(uc.getMessage(["background/location"])), 2)
    self.assertEqual(len(uc.getMessage(["background/activity"])), 2)
    self.assertEqual(len(uc.getMessage(["background/accelerometer"])), 2)
    self.assertEqual(uc.getMessage(["background/location"])[1]["data"]["mElapsedTime"], 142233)
    self.assertEqual(uc.getMessage(["background/activity"])[1]["data"]["mode"], "cycling")
    self.assertEqual(uc.getMessage(["background/accelerometer"])[1]["data"]["x"], 2345)

  def testClearBackgroundData(self):
    start_ts = time.time()
    background_data_from_phone = [
      {
        "metadata": {
          "write_ts": time.time(),
          "type": "message",
          "key": "background/location",
        },
        "data" : { "mLat": 45.64, "mLng": 21.35, "mElapsedTime": 112233, }
      },
      {
        "metadata": {
          "write_ts": time.time(),
          "type": "message",
          "key": "background/activity",
        },
        "data" : { "mode": "walking", "confidence": 90 }
      },
      {
        "metadata": {
          "write_ts": time.time(),
          "type": "message",
          "key": "background/accelerometer",
        },
        "data" : {"x": 1234, "y": 2345, "z": 3456}
      },
    ]

    # sleep() expects an argument in seconds. We want to sleep for 5 ms.
    time.sleep(old_div(float(5),1000))

    background_data_from_phone_2 = [
      {
        "metadata": {
          "write_ts": time.time(),
          "type": "message",
          "key": "background/location",
        },
        "data" : { "mLat": 49.64, "mLng": 25.35, "mElapsedTime": 142233, }
      },
      {
        "metadata": {
          "write_ts": time.time(),
          "type": "message",
          "key": "background/activity",
        },
        "data" : { "mode": "cycling", "confidence": 70 }
      },
      {
        "metadata": {
          "write_ts": time.time(),
          "type": "message",
          "key": "background/accelerometer",
        },
        "data" : {"x": 2345, "y": 3456, "z": 4567}
      },
    ]

    # We look for entries that are > 5 secs old, so it is fine to set the
    # end_ts after all the entries have been inserted.
    # If we don't add the fuzz factor of 5 seconds, then we sometimes end up
    # with the first background entry having the same timestamp as end_ts
    # and the counts don't work. We have a fuzz factor in the real world - lets'
    # add one here as well

    end_ts = time.time()

    time.sleep(old_div(float(5), 1000))

    background_data_from_phone_3 = [
      {
        "metadata": {
          "write_ts": time.time(),
          "type": "message",
          "key": "background/location",
        },
        "data" : { "mLat": 45.64, "mLng": 21.35, "mElapsedTime": 112233, }
      },
      {
        "metadata": {
          "write_ts": time.time() + 30,
          "type": "message",
          "key": "background/location",
        },
        "data" : { "mLat": 49.64, "mLng": 25.35, "mElapsedTime": 142233, }
      },
      {
        "metadata": {
          "write_ts": time.time(),
          "type": "message",
          "key": "background/activity",
        },
        "data" : { "mode": "walking", "confidence": 90 }
      },
      {
        "metadata": {
          "write_ts": time.time() + 30,
          "type": "message",
          "key": "background/activity",
        },
        "data" : { "mode": "cycling", "confidence": 70 }
      },
      {
        "metadata": {
          "write_ts": time.time(),
          "type": "message",
          "key": "background/accelerometer",
        },
        "data" : {"x": 1234, "y": 2345, "z": 3456}
      },
      {
        "metadata": {
          "write_ts": time.time() + 30,
          "type": "message",
          "key": "background/accelerometer",
        },
        "data" : {"x": 2345, "y": 3456, "z": 4567}
      },
    ]

    mauc.sync_phone_to_server(self.testUserUUID, background_data_from_phone)
    mauc.sync_phone_to_server(self.testUserUUID, background_data_from_phone_2)
    mauc.sync_phone_to_server(self.testUserUUID, background_data_from_phone_3)

    uc = ucauc.UserCache.getUserCache(self.testUserUUID)

    tq = estt.TimeQuery("metadata.write_ts", start_ts, end_ts)
    self.assertEqual(len(uc.getMessage(["background/location"], tq)), 2)
    self.assertEqual(len(uc.getMessage(["background/activity"], tq)), 2)
    self.assertEqual(len(uc.getMessage(["background/accelerometer"], tq)), 2)
  
    uc.clearProcessedMessages(tq, ["background/location", "background/accelerometer"])
    self.assertEqual(len(uc.getMessage(["background/accelerometer"], tq)), 0)
    self.assertEqual(len(uc.getMessage(["background/location"], tq)), 0)
    self.assertEqual(len(uc.getMessage(["background/activity"], tq)), 2)

  def testGetUUIDList(self):
    self.testGetTwoSetsOfUserDataFromPhone()
    uuid_list = ucauc.UserCache.get_uuid_list()
    self.assertEqual(uuid_list, [self.testUserUUID])

if __name__ == '__main__':
    import emission.tests.common as etc

    etc.configLogging()
    unittest.main()
