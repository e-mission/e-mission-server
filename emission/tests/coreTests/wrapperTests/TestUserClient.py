# Standard imports
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

class TestUserClient(unittest.TestCase):
  def setUp(self):
    import emission.core.get_database as edb
    common.dropAllCollections(edb.get_db())

  def testCountForStudy(self):
    client = Client("testclient")
    client.update(createKey = False)
    common.makeValid(client)

    (resultPre, resultReg) = client.preRegister("this_is_the_super_secret_id", "fake@fake.com")
    self.assertEqual(resultPre, 1)
    self.assertEqual(resultReg, 0)

    user = User.register('fake@fake.com')
    self.assertEquals(userclient.countForStudy('testclient'), 1)

  def testCountForStudyDefault(self):
    user = User.register('fake@fake.com')
    self.assertEquals(userclient.countForStudy('testclient'), 0)
    self.assertEquals(userclient.countForStudy(None), 1)

  def testClientQuery(self):
    self.assertEquals(userclient.getClientQuery(None), {'study_list': {'$size': 0}});
    self.assertEquals(userclient.getClientQuery('testclient'), {'study_list': {'$in': ['testclient']}});

if __name__ == '__main__':
    etc.configLogging()
    unittest.main()
