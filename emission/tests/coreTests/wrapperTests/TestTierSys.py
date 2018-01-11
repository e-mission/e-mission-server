# Standard imports
import unittest
import sys
import os
from datetime import datetime, timedelta
import logging

# Our imports
from emission.core.wrapper.tiersys import TierSys
from emission.tests import common
import emission.tests.common as etc
import emission.core.get_database as edb
import emission.storage.timeseries.abstract_timeseries as esta
import pandas as pd
import arrow
from emission.core.get_database import get_tiersys_db

import emission.tests.common as etc

class TestTierSys(unittest.TestCase):
  def setUp(self):
      #etc.dropAllCollections(edb.get_db())
      return

  def testAddTier(self):
      ts = TierSys(0)
      num_tiers = len(ts.tiers)
      self.assertEquals(num_tiers, 0, "Did not correctly initialize number of tiers.")

      # Basic test, adds a dictionary key value pair.
      ts.addTier(1)
      num_tiers = len(ts.tiers)
      self.assertEquals(num_tiers, 1, "Did not correctly add a tier.")

      # Test that exception raised when attempting to add a rank that already exists.
      with self.assertRaises(Exception) as context:
          ts.addTier(1)
      return

  def testComputePenalty(self):
      """
      IN_VEHICLE = 0, BICYCLING = 1, ON_FOOT = 2, STILL = 3, UNKNOWN = 4, TILTING = 5, WALKING = 7, RUNNING = 8
      """
      ts = TierSys(0)
      d = {'sensed_mode': [0, 1, 0, 2, 3, 4, 5, 6, 7, 8, 1], 'distance': [3.32, 4.45, 0.63, 1.214, 5.43, 0, 1.3, 65.2, 32.2, 31.3, 8.31]}
      #should be 37.5 - 3.32 + 37.5 - 0.63 + 5 - 4.45 + 0...
      penalty_df = pd.DataFrame(data=d)
      self.assertEqual(ts.computePenalty(penalty_df), 71.6, "Many inputs penalty is not 71.6")

      d = {'sensed_mode': [], 'distance': []}
      penalty_df = pd.DataFrame(data=d)
      self.assertEqual(ts.computePenalty(penalty_df), 0, "No inputs penalty is not 0")
      return

  def testComputeFootprint(self):
      ts = TierSys(0)
      d = {'sensed_mode': [0, 1, 2], 'distance': [100.1, 2000.1, 80.1]}
      footprint_df = pd.DataFrame(data=d)
      ans = (float(92)/1609 + float(287)/1609)/(2 * 1000) * 100.1
      self.assertEqual(ts.computeFootprint(footprint_df), ans, "Many inputs footprint is not 1178.927")

      d = {'sensed_mode': [1, 2], 'distance': [100.1, 2000.1]}
      footprint_df = pd.DataFrame(data=d)
      self.assertEqual(ts.computeFootprint(footprint_df), 0, "Expected non vehicle input to have no footprint")

      d = {'sensed_mode': [], 'distance': []}
      footprint_df = pd.DataFrame(data=d)
      self.assertEqual(ts.computeFootprint(footprint_df), 0, "Expected no inputs to yield 0")
      return

  def testComputeCarbon(self):
      etc.setupRealExample(self, "emission/tests/data/real_examples/shankari_2015-07-22")
      etc.runIntakePipeline(self.testUUID)

      self.ts = esta.TimeSeries.get_time_series(self.testUUID)
      cs_df = self.ts.get_data_df("analysis/cleaned_section", time_query=None)
      pd.set_option('display.max_columns', None)
      print(cs_df.shape)
      print(cs_df[['sensed_mode', 'distance']])

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

      latest = TierSys.getLatest()
      for a in TierSys.getLatest():
          self.assertEqual(a['tiers'][0], {'rank': 1, 'uuids': [4, 5, 6]}, "Did not get latest Tier")
          self.assertEqual(a['tiers'][1], {'rank': 2, 'uuids': [1, 2, 3]}, "Did not get latest Tier")

      return

if __name__ == '__main__':
    etc.configLogging()
    unittest.main()
