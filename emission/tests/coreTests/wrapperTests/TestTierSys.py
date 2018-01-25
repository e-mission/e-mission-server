# Standard imports
import unittest
import sys
import os
from datetime import datetime, timedelta
import logging

# Our imports
from emission.core.wrapper.tiersys import TierSys
from emission.core.wrapper.user import User
from emission.tests import common
import emission.tests.common as etc
import emission.core.get_database as edb
import emission.storage.timeseries.abstract_timeseries as esta
import pandas as pd
import arrow
from emission.core.get_database import get_tiersys_db
import uuid

import emission.tests.common as etc

class TestTierSys(unittest.TestCase):
  def setUp(self):
      etc.dropAllCollections(edb._get_current_db())
      return

  def testAddTier(self):
      ts = TierSys(0)
      num_tiers = len(ts.tiers)
      self.assertEqual(num_tiers, 0, "Did not correctly initialize number of tiers.")

      # Basic test, adds a dictionary key value pair.
      ts.addTier(1)
      num_tiers = len(ts.tiers)
      self.assertEqual(num_tiers, 1, "Did not correctly add a tier.")

      # Test that exception raised when attempting to add a rank that already exists.
      with self.assertRaises(Exception) as context:
          ts.addTier(1)
      return

  def testDeleteTier(self):
      ts = TierSys(5)
      num_tiers = len(ts.tiers)
      self.assertEqual(num_tiers, 5, "Did not correctly initialize number of tiers.")

      # Basic test, adds a dictionary key value pair.
      ts.deleteTier(2)
      num_tiers = len(ts.tiers)
      self.assertEqual(num_tiers, 4, "Did not correctly delete a tier.")

      # Test that exception raised when attempting to add a rank that already exists.
      with self.assertRaises(Exception) as context:
          ts.deleteTier(2)
      return

  def testComputeCarbon(self):
      etc.setupRealExample(self, "emission/tests/data/real_examples/shankari_2015-07-22")
      etc.runIntakePipeline(self.testUUID)
      #total dist: 51630.268529
      #carbonfootprint:
      #penalty:
      tiersys = TierSys(0)
      time = arrow.Arrow(2010,5,1).timestamp

      totalDist = 1354.505326 + 976.425010 + 7099.545921 + 378.832636 + \
      3435.018524 + 1243.291266 + 7854.830970 + 703.250545 + 447.809170 + \
      1023.561199 + 1149.518965 + 7946.758593 + 603.111034 + 885.495073 + \
      1101.908416 + 1190.394822 + 1263.283264 + 7365.816160 + 1725.298455 + \
      887.985382 + 2993.627798
      footprint = float((92 + 287)/2)/float(1609) * float((7099.545921 + 7854.830970 + 7946.758593 + 7365.816160))/float(1000)
      penalty = float(37.5*4*1609.344)/float(1000) - float((7099.545921 + 7854.830970 + 7946.758593 + \
      7365.816160))/float(1000)

      ts = esta.TimeSeries.get_time_series(self.testUUID)
      cs_df = ts.get_data_df("analysis/cleaned_section")

      self.assertEqual(int(User.computeFootprint(cs_df) * 10000), int(footprint * 10000), "footprint value is not correct")
      self.assertEqual(int(User.computePenalty(cs_df) * 10000), int(penalty * 10000), "penalty value is not correct")

      val = float(footprint + penalty)/float(totalDist)
      self.assertEqual(int(tiersys.computeCarbon(self.testUUID, time) * 10000), int(val * 10000), "computeCarbon Fails")
      return

  def testGetLatest(self):
      from datetime import datetime
      from datetime import timedelta
      """
      Saves the current tiers into the tiersys.
      Adds/Replaces array of tiers into tiersys object
      Gets current array of uuids and puts them into tier objects
      {{
          _id : DEFINED BY MONGO,
          created_at: datetime,
          tiers : [{
              rank : [],
              uuids : []
          }, {
              rank : [],
              uuids : []
          }]
      }}
      """
      tiers = [(1, [1, 2, 3]), (2, [4, 5, 6])]
      correctTiers = [(1, [4, 5, 6]), (2, [1, 2, 3])]
      ts = []
      correctTs = []
      for rank, users in tiers:
          ts.append({'rank': rank, 'uuids': users})

      for rank, users in correctTiers:
          correctTs.append({'rank': rank, 'uuids': users})

      get_tiersys_db().insert_one({'tiers': ts, 'created_at': datetime.now() - timedelta(hours = 1)})
      get_tiersys_db().insert_one({'tiers': ts, 'created_at': datetime.now() - timedelta(hours = 2)})
      get_tiersys_db().insert_one({'tiers': ts, 'created_at': datetime.now() - timedelta(hours = 3)})
      get_tiersys_db().insert_one({'tiers': correctTs, 'created_at': datetime.now()})

      for a in TierSys.getLatest():
          self.assertEqual(a['tiers'][0], {'rank': 1, 'uuids': [4, 5, 6]}, "Did not get latest Tier")
          self.assertEqual(a['tiers'][1], {'rank': 2, 'uuids': [1, 2, 3]}, "Did not get latest Tier")
      return

  def testDivideIntoBuckets(self):
      items = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
      with self.assertRaises(Exception) as context:
          TierSys.divideIntoBuckets(items, 0)
      self.assertEqual(TierSys.divideIntoBuckets(items, 1), [[0, 1, 2, 3, 4, 5, 6, 7, 8, 9]])
      self.assertEqual(TierSys.divideIntoBuckets(items, 2), [[0, 1, 2, 3, 4], [5, 6, 7, 8, 9]])
      self.assertEqual(TierSys.divideIntoBuckets(items, 3), [[0, 1, 2, 3], [4, 5, 6], [7, 8, 9]])
      self.assertEqual(TierSys.divideIntoBuckets(items, 4), [[0, 1, 2], [3, 4, 5], [6, 7], [8, 9]])
      self.assertEqual(TierSys.divideIntoBuckets(items, 5), [[0, 1], [2, 3], [4, 5], [6, 7], [8, 9]])
      self.assertEqual(TierSys.divideIntoBuckets(items, 6), [[0, 1], [2, 3], [4, 5], [6, 7], [8], [9]])
      self.assertEqual(TierSys.divideIntoBuckets(items, 7), [[0, 1], [2, 3], [4, 5], [6], [7], [8], [9]])
      self.assertEqual(TierSys.divideIntoBuckets(items, 8), [[0, 1], [2, 3], [4], [5], [6], [7], [8], [9]])
      self.assertEqual(TierSys.divideIntoBuckets(items, 9), [[0, 1], [2], [3], [4], [5], [6], [7], [8], [9]])
      self.assertEqual(TierSys.divideIntoBuckets(items, 10), [[0], [1], [2], [3], [4], [5], [6], [7], [8], [9]])
      with self.assertRaises(Exception) as context:
          TierSys.divideIntoBuckets(items, 11)
      return

  def testComputeRanks(self):
      time = arrow.Arrow(2010,5,1).timestamp
      tiersys = TierSys(0)

      etc.setupRealExample(self, "emission/tests/data/real_examples/shankari_2015-08-23")
      shankari8_id = self.testUUID
      etc.runIntakePipeline(self.testUUID)
      shankari8_carbon = tiersys.computeCarbon(shankari8_id, time)
      User.registerWithUUID("shankari8@gmail.com", shankari8_id)
      etc.setupRealExample(self, "emission/tests/data/real_examples/shankari_2015-07-22")
      shankari7_id = self.testUUID
      etc.runIntakePipeline(self.testUUID)
      shankari7_carbon = tiersys.computeCarbon(shankari7_id, time)
      User.registerWithUUID("shankari7@gmail.com", shankari7_id)
      user_tiers = tiersys.computeRanks(time, 2)
      self.assertEqual(user_tiers, [[shankari7_id], [shankari8_id]])
      return

  def testUpdateAndSaveTiers(self):
      time = arrow.Arrow(2010,5,1).timestamp
      tiersys = TierSys(0)

      etc.setupRealExample(self, "emission/tests/data/real_examples/shankari_2016-06-20")
      etc.runIntakePipeline(self.testUUID)
      shankari0620 = self.testUUID
      User.registerWithUUID("shankari06-20@gmail.com", self.testUUID)

      etc.setupRealExample(self, "emission/tests/data/real_examples/shankari_2016-06-21")
      etc.runIntakePipeline(self.testUUID)
      shankari0621 = self.testUUID
      User.registerWithUUID("shankari06-21gmail.com", self.testUUID)

      etc.setupRealExample(self, "emission/tests/data/real_examples/shankari_2016-07-22")
      etc.runIntakePipeline(self.testUUID)
      shankari0722 = self.testUUID
      User.registerWithUUID("shankari07-22@gmail.com", self.testUUID)

      etc.setupRealExample(self, "emission/tests/data/real_examples/shankari_2016-07-25")
      etc.runIntakePipeline(self.testUUID)
      shankari0725 = self.testUUID
      User.registerWithUUID("shankari07-25@gmail.com", self.testUUID)

      etc.setupRealExample(self, "emission/tests/data/real_examples/shankari_2016-07-27")
      etc.runIntakePipeline(self.testUUID)
      shankari0727 = self.testUUID
      User.registerWithUUID("shankari07-27@gmail.com", self.testUUID)

      tiersys.updateTiers(time)
      tiers = tiersys.tiers
      print(tiers)
      self.assertEqual(True, shankari0621 in tiers[1] and shankari0725 in tiers[1])
      self.assertEqual(True, shankari0722 in tiers[2] and shankari0727 in tiers[2])
      self.assertEqual(True, shankari0620 in tiers[3])

      tiersys.saveTiers()

      for a in TierSys.getLatest():
          self.assertEqual(a['tiers'][2], {'rank': 3, 'uuids': [shankari0620]}, "Save failed")
      return


if __name__ == '__main__':
    etc.configLogging()
    unittest.main()
