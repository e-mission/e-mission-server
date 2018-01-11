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
      etc.dropAllCollections(edb._get_current_db())
      return

  def testComputePenalty(self):
      """
      IN_VEHICLE = 0, BICYCLING = 1, ON_FOOT = 2, STILL = 3, UNKNOWN = 4, TILTING = 5, WALKING = 7, RUNNING = 8
      """
      ts = TierSys(0)
      d = {'sensed_mode': [0, 1, 0, 2, 3, 4, 5, 6, 7, 8, 1], 'distance': [3320, 4450, 630, 12140, 5430, 0, 1300, 65200, 32200, 31300, 8310]}
      penalty_df = pd.DataFrame(data=d)
      penalty = (37.5 * 1609.344/1000) - 3.320 + (37.5 * 1609.344/1000) - 0.630
      self.assertEqual(ts.computePenalty(penalty_df), penalty, "Many inputs penalty is not correct")

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

      self.assertEqual(int(tiersys.computeFootprint(cs_df) * 10000), int(footprint * 10000), "footprint value is not correct")
      self.assertEqual(int(tiersys.computePenalty(cs_df) * 10000), int(penalty * 10000), "penalty value is not correct")

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

      latest = TierSys.getLatest()
      for a in TierSys.getLatest():
          self.assertEqual(a['tiers'][0], {'rank': 1, 'uuids': [4, 5, 6]}, "Did not get latest Tier")
          self.assertEqual(a['tiers'][1], {'rank': 2, 'uuids': [1, 2, 3]}, "Did not get latest Tier")
      return

if __name__ == '__main__':
    etc.configLogging()
    unittest.main()
