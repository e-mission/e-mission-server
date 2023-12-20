from __future__ import division
from __future__ import unicode_literals
from __future__ import print_function
from __future__ import absolute_import
# Standard imports
from future import standard_library
standard_library.install_aliases()
from builtins import *
import unittest

# Our imports
from emission.core.wrapper.user import User
import emission.tests.common as etc
import emission.core.get_database as edb

import emission.tests.common as etc

class TestUserCustomMode(unittest.TestCase):

  def setUp(self):
    etc.dropAllCollections(edb._get_current_db())
    self.user = User.register('fake@fake.com')

  def testinitialGetUserCustomModes(self):
    self.assertListEqual(self.user.getUserCustomModes(), [])

  def testInsertUserCustomMode(self):
    updated_mode = {
      'old_mode' : '',
      'new_mode' : 'test1',
      'is_new_mode_must_added': True
    }
    mode = self.user.updateUserCustomMode(updated_mode)
    self.assertListEqual(mode, ['test1'])

  def testUpdateUserCustomMode(self):
    self.testInsertUserCustomMode()
    updated_mode = {
      'old_mode' : 'test1',
      'new_mode' : 'test2',
      'is_new_mode_must_added': True
    }
    mode = self.user.updateUserCustomMode(updated_mode)
    self.assertListEqual(mode, ['test2', 'test1'])

  def testDeleteUserCustomMode(self):
    self.testInsertUserCustomMode()
    mode = self.user.deleteUserCustomMode('test1')
    self.assertListEqual(mode, [])


if __name__ == '__main__':
    etc.configLogging()
    unittest.main()

