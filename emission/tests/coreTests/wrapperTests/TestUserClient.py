# Standard imports
import unittest
import sys
import os
from datetime import datetime, timedelta
import logging
logging.basicConfig(level=logging.DEBUG)

# Our imports
from emission.core.wrapper.user import User
from emission.core.wrapper.client import Client
from tests import common
from emission.analysis.results import userclient

class TestUserClient(unittest.TestCase):
  def setUp(self):
    from get_database import get_profile_db, get_uuid_db, get_client_db, get_pending_signup_db
    # Make sure we start with a clean slate every time
    get_client_db().remove()
    get_profile_db().remove()
    get_pending_signup_db().remove()
    get_uuid_db().remove()

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
    from get_database import get_profile_db, get_uuid_db, get_client_db, get_pending_signup_db
    user = User.register('fake@fake.com')
    self.assertEquals(userclient.countForStudy('testclient'), 0)
    self.assertEquals(userclient.countForStudy(None), 1)

  def testClientQuery(self):
    self.assertEquals(userclient.getClientQuery(None), {'study_list': {'$size': 0}});
    self.assertEquals(userclient.getClientQuery('testclient'), {'study_list': {'$in': ['testclient']}});

if __name__ == '__main__':
    unittest.main()
