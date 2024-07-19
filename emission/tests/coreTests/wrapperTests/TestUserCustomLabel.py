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
    self.user = User.register('fake@fake.com')

  def testInitialGetUserCustomLabels(self):
    self.assertListEqual(self.user.getUserCustomLabel('mode'), [])
    self.assertListEqual(self.user.getUserCustomLabel('purpose'), [])

  def testInsertCustomLabel(self):
    inserted_mode = {
      'key' : 'mode',
      'label' : 'mode1',
    }
    mode = self.user.insertUserCustomLabel(inserted_mode)
    self.assertListEqual(mode, ['mode1'])

    inserted_purpose = {
      'key' : 'purpose',
      'label' : 'purpose1',
    }
    purpose = self.user.insertUserCustomLabel(inserted_purpose)
    self.assertListEqual(purpose, ['purpose1'])

  def tesUpdateUserCustomLabel(self):
    self.testInsertCustomLabel()
    updated_mode = {
      'key' : 'mode',
      'old_label' : '',
      'new_label' : 'mode2',
      'is_new_label_must_added': True
    }
    mode = self.user.updateUserCustomLabel(updated_mode)
    self.assertListEqual(mode, ['mode2', 'mode1'])

    updated_purpose = {
      'key' : 'purpose',
      'old_label' : '',
      'new_label' : 'purpose2',
      'is_new_label_must_added': True
    }
    purpose = self.user.updateUserCustomLabel(updated_purpose)
    self.assertListEqual(purpose, ['purpose2', 'purpose1'])

  def testDeleteUserCustomMode(self):
    self.tesUpdateUserCustomLabel()
    deleted_mode = {
      'key' : 'mode',
      'label' : 'mode2',
    }
    mode = self.user.deleteUserCustomLabel(deleted_mode)

    self.assertListEqual(mode, ['mode1'])

    deleted_purpose = {
      'key' : 'purpose',
      'label' : 'purpose2',
    }
    purpose = self.user.deleteUserCustomLabel(deleted_purpose)
    self.assertListEqual(purpose, ['purpose1'])

  def tearDown(self):
    etc.dropAllCollections(edb._get_current_db())


if __name__ == '__main__':
    etc.configLogging()
    unittest.main()

