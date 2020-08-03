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
import sys
import os
from datetime import datetime, timedelta
import logging

# Our imports
from emission.core.wrapper.user import User
from emission.core.wrapper.client import Client
from emission.tests import common
from emission.analysis.result import userclient
import emission.tests.common as etc
import emission.core.get_database as edb

import emission.tests.common as etc
import pandas as pd

class TestUser(unittest.TestCase):
  def setUp(self):
    etc.dropAllCollections(edb._get_current_db())

  def testIsNotRegistered(self):
    self.assertFalse(User.isRegistered('fake@fake.com'))

  def testRegisterUser(self):
    user = User.register('fake@fake.com')
    self.assertTrue(User.isRegistered('fake@fake.com'))

  def testGetAvgMpg(self):
    user = User.register('fake@fake.com')
    user.setMpgArray([45, 50, 31])
    self.assertEqual(user.getAvgMpg(), 42)

  def testGetCarbonFootprintForMode(self):
    user = User.register('fake@fake.com')
    user.setMpgArray([45, 50, 31])
    # Avg MPG = 42
    correctCarbonFootprintForMode = {'walking' : 0,
                          'running' : 0,
                          'cycling' : 0,
                            'mixed' : 0,
                        'bus_short' : old_div(267.0,1609),
                         'bus_long' : old_div(267.0,1609),
                      'train_short' : old_div(92.0,1609),
                       'train_long' : old_div(92.0,1609),
                        'car_short' : (old_div(1,(42*1.6093)))*8.91,
                         'car_long' : (old_div(1,(42*1.6093)))*8.91,
                        'air_short' : old_div(217.0,1609),
                         'air_long' : old_div(217.0,1609)
                      }
    self.assertEqual(user.getCarbonFootprintForMode(), correctCarbonFootprintForMode)

  def testMergeDict(self):
    dict1 = {'a': 'a1', 'b': 'b1', 'c': 'c1'}
    dict2 = {'d': 'd2', 'b': 'b2', 'c': 'c2'}
    mergedDict = User.mergeDicts(dict1, dict2)

    self.assertEqual(len(mergedDict), 4)
    self.assertEqual(mergedDict['a'], 'a1')
    self.assertEqual(mergedDict, {'a': 'a1', 'b': 'b2', 'c': 'c2', 'd': 'd2'})

  def testGetSettingsDefaultUser(self):
    user = User.register('fake@fake.com')
    self.assertRegex(user.getSettings()['result_url'], ".*/compare")

  def testRegisterExistingUser(self):
    user = User.register('fake@fake.com')
    # Here's the key difference, now register again
    user = User.register('fake@fake.com')
    self.assertTrue(User.isRegistered("fake@fake.com"))

  def testUnregister(self):
    user = User.register('fake@fake.com')
    self.assertTrue(User.isRegistered('fake@fake.com'))
    User.unregister('fake@fake.com')
    self.assertFalse(User.isRegistered('fake@fake.com'))

  def testChangeUpdateTs(self):
    from datetime import datetime, timedelta

    user = User.register('fake@fake.com')
    self.assertTrue(User.isRegistered('fake@fake.com'))
    user.changeUpdateTs(timedelta(days = -20))
    self.assertEqual((datetime.now() - user.getUpdateTS()).days, 20)

  def testComputePenalty(self):
    """
    IN_VEHICLE = 0, BICYCLING = 1, ON_FOOT = 2, STILL = 3, UNKNOWN = 4, TILTING = 5, WALKING = 7, RUNNING = 8
    """
    d = {'sensed_mode': [0, 1, 0, 2, 3, 4, 5, 6, 7, 8, 1], 'distance': [3320, 4450, 630, 12140, 5430, 0, 1300, 65200, 32200, 31300, 8310]}
    penalty_df = pd.DataFrame(data=d)
    penalty = (37.5 * 1609.344/1000) - 3.320 + (37.5 * 1609.344/1000) - 0.630
    self.assertEqual(User.computePenalty(penalty_df), penalty, "Many inputs penalty is not correct")

    d = {'sensed_mode': [], 'distance': []}
    penalty_df = pd.DataFrame(data=d)
    self.assertEqual(User.computePenalty(penalty_df), 0, "No inputs penalty is not 0")
    return

  def testComputeFootprint(self):
    d = {'sensed_mode': [0, 1, 2], 'distance': [100.1, 2000.1, 80.1]}
    footprint_df = pd.DataFrame(data=d)
    ans = (float(92)/1609 + float(287)/1609)/(2 * 1000) * 100.1
    self.assertEqual(User.computeFootprint(footprint_df), ans, "Many inputs footprint is not 1178.927")

    d = {'sensed_mode': [1, 2], 'distance': [100.1, 2000.1]}
    footprint_df = pd.DataFrame(data=d)
    self.assertEqual(User.computeFootprint(footprint_df), 0, "Expected non vehicle input to have no footprint")

    d = {'sensed_mode': [], 'distance': []}
    footprint_df = pd.DataFrame(data=d)
    self.assertEqual(User.computeFootprint(footprint_df), 0, "Expected no inputs to yield 0")
    return

if __name__ == '__main__':
    etc.configLogging()
    unittest.main()
