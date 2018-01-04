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
import pandas as pd

import emission.tests.common as etc

class TestTierSys(unittest.TestCase):
  def setUp(self):
      return

  def testComputePenalty(self):
      """
      IN_VEHICLE = 0, BICYCLING = 1, ON_FOOT = 2, STILL = 3, UNKNOWN = 4, TILTING = 5, WALKING = 7, RUNNING = 8
      """
      ts = TierSys(0)
      d = {'sensed_mode': [0, 1, 0, 2, 3, 4, 5, 6, 7, 8, 1], 'distance': [3.32, 4.45, 0.63, 1.214, 5.43, 0, 1.3, 65.2, 32.2, 31.3, 8.31]}
      #should be 37.5 - 3.32 + 37.5 - 0.63 + 5 - 4.45 + 0...
      penalty_df = pd.DataFrame(data=d)
      self.assertEquals(ts.computePenalty(penalty_df), 71.6, "Many inputs penalty is not 71.6")

      d = {'sensed_mode': [], 'distance': []}
      penalty_df = pd.DataFrame(data=d)
      self.assertEquals(ts.computePenalty(penalty_df), 0, "No inputs penalty is not 0")
      return

  def testComputeFootprint(self):
      ts = TierSys(0)
      d = {'sensed_mode': [0, 1, 2], 'distance': [100.1, 2000.1, 80.1]}
      footprint_df = pd.DataFrame(data=d)
      ans = (float(92)/1609 + float(287)/1609)/(2 * 1000) * 100.1
      self.assertEquals(ts.computeFootprint(footprint_df), ans, "Many inputs footprint is not 1178.927")

      d = {'sensed_mode': [1, 2], 'distance': [100.1, 2000.1]}
      footprint_df = pd.DataFrame(data=d)
      self.assertEquals(ts.computeFootprint(footprint_df), 0, "Expected non vehicle input to have no footprint")

      d = {'sensed_mode': [], 'distance': []}
      footprint_df = pd.DataFrame(data=d)
      self.assertEquals(ts.computeFootprint(footprint_df), 0, "Expected no inputs to yield 0")
      return

  def testComputeCarbon(self):
      return

if __name__ == '__main__':
    etc.configLogging()
    unittest.main()
