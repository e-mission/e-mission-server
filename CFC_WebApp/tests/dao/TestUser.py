import unittest
import sys
import os
from datetime import datetime, timedelta

sys.path.append("%s" % os.getcwd())
from dao.user import User
from dao.client import Client
from tests import common
from main import userclient

import logging
logging.basicConfig(level=logging.DEBUG)

class TestUser(unittest.TestCase):
  def setUp(self):
    from get_database import get_profile_db, get_uuid_db, get_client_db, get_pending_signup_db
    # Make sure we start with a clean slate every time
    get_client_db().remove()
    get_profile_db().remove()
    get_pending_signup_db().remove()
    get_uuid_db().remove()

  def testIsNotRegistered(self):
    self.assertFalse(User.isRegistered('fake@fake.com'))

  def testCountForStudyZero(self):
    self.assertEquals(userclient.countForStudy('testclient'), 0)

  def testRegisterNonStudyUser(self):
    user = User.register('fake@fake.com')
    self.assertEquals(user.getStudy(), [])

  def testRegisterStudyUser(self):
    client = Client("testclient")
    client.update(createKey = False)
    common.makeValid(client)

    (resultPre, resultReg) = client.preRegister("this_is_the_super_secret_id", "fake@fake.com")
    self.assertEqual(resultPre, 1)
    self.assertEqual(resultReg, 0)

    user = User.register('fake@fake.com')
    self.assertEquals(user.getStudy(), ['testclient'])

  def testIsRegistered(self):
    user = User.register('fake@fake.com')
    self.assertTrue(User.isRegistered('fake@fake.com'))

  def testSetStudy(self):
    user = User.register('fake@fake.com')
    user.setStudy('testclient')
    self.assertEquals(userclient.countForStudy('testclient'), 1)

  def testUnsetStudyExists(self):
    user = User.register('fake@fake.com')
    user.setStudy('testclient')
    self.assertEquals(userclient.countForStudy('testclient'), 1)

    user.unsetStudy('testclient')
    self.assertEquals(userclient.countForStudy('testclient'), 0)

  def testUnsetStudyNotExists(self):
    user = User.register('fake@fake.com')
    user.unsetStudy('testclient')
    self.assertEquals(userclient.countForStudy('testclient'), 0)

  def testMergeDict(self):
    dict1 = {'a': 'a1', 'b': 'b1', 'c': 'c1'}
    dict2 = {'d': 'd2', 'b': 'b2', 'c': 'c2'}
    mergedDict = User.mergeDicts(dict1, dict2)
    
    self.assertEqual(len(mergedDict), 4)
    self.assertEqual(mergedDict['a'], 'a1')
    self.assertEqual(mergedDict, {'a': 'a1', 'b': 'b2', 'c': 'c2', 'd': 'd2'})

  def testGetSettingsCustomUser(self):
    client = Client("testclient")
    client.update(createKey = False)
    common.makeValid(client)

    (resultPre, resultReg) = client.preRegister("this_is_the_super_secret_id", "fake@fake.com")
    self.assertEqual(resultPre, 1)
    self.assertEqual(resultReg, 0)

    user = User.register('fake@fake.com')
    self.assertRegexpMatches(user.getSettings()['result_url'], ".*/test/test/test")

  def testGetSettingsDefaultUser(self):
    user = User.register('fake@fake.com')
    self.assertRegexpMatches(user.getSettings()['result_url'], ".*/compare")

  def testGetSettingsExpiredUser(self):
    user = User.register('fake@fake.com')
    self.assertRegexpMatches(user.getSettings()['result_url'], ".*/compare")

    client = Client("testclient")
    client.update(createKey = False)
    common.makeValid(client)

    (resultPre, resultReg) = client.preRegister("this_is_the_super_secret_id", "fake@fake.com")
    self.assertRegexpMatches(user.getSettings()['result_url'], ".*/test/test/test")

    common.makeExpired(client)
    self.assertRegexpMatches(user.getSettings()['result_url'], ".*/compare")

  def testRegisterExistingUser(self):
    user = User.register('fake@fake.com')
    self.assertEquals(user.getStudy(), [])
    
    client = Client("testclient")
    client.update(createKey = False)
    common.makeValid(client)

    (resultPre, resultReg) = client.preRegister("this_is_the_super_secret_id", "fake@fake.com")
    self.assertEqual(resultPre, 0)
    self.assertEqual(resultReg, 1)

    user = User.fromEmail("fake@fake.com")
    self.assertEquals(user.getStudy(), ['testclient'])

    # Here's the key difference, now register again
    user = User.register('fake@fake.com')
    self.assertEquals(user.getStudy(), ['testclient'])

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

  def testGetFirstStudy(self):
    user = User.register('fake@fake.com')
    self.assertTrue(User.isRegistered('fake@fake.com'))

    client = Client("testclient")
    client.update(createKey = False)
    common.makeValid(client)

    (resultPre, resultReg) = client.preRegister("this_is_the_super_secret_id", "fake@fake.com")
    self.assertEqual(resultPre, 0)
    self.assertEqual(resultReg, 1)

    user = User.fromEmail('fake@fake.com')
    self.assertEqual(user.getFirstStudy(), 'testclient')

  def testSetClientSpecificFields(self):
    user = User.register('fake@fake.com')
    self.assertTrue(User.isRegistered('fake@fake.com'))

    # Check that the field doesn't exist initially    
    self.assertTrue(user.getProfile().get('test_field', 'blank'), 'blank')

    # Check that a simple value update works
    user.setClientSpecificProfileFields({'test_field': 'something beautiful'})
    self.assertTrue(user.getProfile().get('test_field', 'blank'), 'something beautiful')

    # Check that a data structure update works
    user.setClientSpecificProfileFields({'test_field': {'something': 'beautiful'}})
    self.assertTrue(user.getProfile().get('test_field', 'blank'), {'something': 'beautiful'})
    
if __name__ == '__main__':
    unittest.main()
