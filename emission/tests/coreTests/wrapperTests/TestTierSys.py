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

import emission.tests.common as etc

class TestTierSys(unittest.TestCase):
  def setUp(self):
      #etc.dropAllCollections(edb.get_db())
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
      #TODO: setupRealExample does not work as expected
      import uuid
      etc.setupRealExample(self, "emission/tests/data/real_examples/shankari_2015-07-22")
      print(self.testUUID)
      self.ts = esta.TimeSeries.get_time_series(self.testUUID)
      print(self.ts.get_uuid_list());
      entry_it = self.ts.find_entries(["analysis/cleaned_trip"])
      #cs_df = self.ts.get_data_df("analysis/cleaned_section")
      for ct in entry_it:
        cte = ecwe.Entry(ct)
        print("found something")
        print "=== Trip:", cte.data.start_loc, "->", cte.data.end_loc
        section_it = esdt.get_sections_for_trip("analysis/cleaned_section", test_user_id, cte.get_id())
        for sec in section_it:
          print "  --- Section:", sec.data.start_loc, "->", sec.data.end_loc, " on ", sec.data.sensed_mode
      #now = arrow.utcnow()
      #yesterday = now.shift(days=-1)
      #ts = TierSys(0)
      #ts.computeCarbon(0, yesterday)
      #analysis/cleaned_section
      #edb.get_timeseries_db
      return

if __name__ == '__main__':
    etc.configLogging()
    unittest.main()
