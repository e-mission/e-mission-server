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
    
if __name__ == '__main__':
    etc.configLogging()
    unittest.main()
