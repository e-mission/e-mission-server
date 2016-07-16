# Standard imports
import unittest
import logging
import json
import uuid
import attrdict as ad
import random

# Our imports
import emission.net.ext_service.habitica.proxy as proxy
import emission.core.get_database as edb

class TestHabiticaRegister(unittest.TestCase):
  def setUp(self):
    print "Test setup called"
    self.testUserUUID = uuid.uuid4()
    autogen_string = randomGen()
    autogen_email = autogen_string + '@test.com'
    self.sampleAuthMessage1 = {'username': autogen_string, 'email': autogen_email, 
      'password': autogen_string, 'our_uuid': self.testUserUUID}

  def tearDown(self):
    # https: // habitica.com / apidoc /  # api-User-UserDelete
    del_result = proxy.habiticaProxy(self.testUserUUID, "DELETE",
                                     "/api/v3/user",
                                     {'password': self.sampleAuthMessage1['password']})
    logging.debug("in tear_down, result = %s" % del_result)

  def testAddNewUser(self):
    sampleAuthMessage1Ad = ad.AttrDict(self.sampleAuthMessage1)
    proxy.habiticaRegister(sampleAuthMessage1Ad.username, sampleAuthMessage1Ad.email,
                           sampleAuthMessage1Ad.password, sampleAuthMessage1Ad.our_uuid)

    find_it = edb.get_habitica_db().find({"user_id": self.testUserUUID})
    self.assertEqual(find_it.count(), 1)
    
    user_val = list(find_it)[0]
    self.assertIsNotNone(user_val['habitica_id'])
    self.assertEqual(user_val['habitica_username'], sampleAuthMessage1Ad.username)

  def testGetUserProfile(self):
      # The user information is randomly generated every time, so
      # every test has to start with creating the user
      sampleAuthMessage1Ad = ad.AttrDict(self.sampleAuthMessage1)
      proxy.habiticaRegister(sampleAuthMessage1Ad.username, sampleAuthMessage1Ad.email,
                             sampleAuthMessage1Ad.password, sampleAuthMessage1Ad.our_uuid)
      ret_profile = proxy.habiticaProxy(self.testUserUUID, "GET",
                                        "/api/v3/user", None)
      ret_json = ret_profile.json()
      logging.debug("Retrieved profile with keys %s" % ret_json.keys())
      logging.debug("profile data keys = %s" % ret_json['data'].keys())
      # User has just been created, so has no gear
      self.assertEqual(ret_json['data']['achievements']['ultimateGearSets'],
                       {'warrior': False, 'rogue': False, 'wizard': False,
                        'healer': False})
      self.assertEqual(ret_json['data']['newMessages'], {})

  def testSleep(self):
      # The user information is randomly generated every time, so
      # every test has to start with creating the user
      sampleAuthMessage1Ad = ad.AttrDict(self.sampleAuthMessage1)
      proxy.habiticaRegister(sampleAuthMessage1Ad.username, sampleAuthMessage1Ad.email,
                             sampleAuthMessage1Ad.password, sampleAuthMessage1Ad.our_uuid)
      ret_sleep = proxy.habiticaProxy(self.testUserUUID, "POST",
                                        "/api/v3/user/sleep",
                                        {'data': True})
      ret_profile = proxy.habiticaProxy(self.testUserUUID, "GET",
                                        "/api/v3/user", None)
      ret_json = ret_profile.json()
      # User should be sleeping
      self.assertEqual(ret_json['data']['preferences']['sleep'], True)

      ret_sleep = proxy.habiticaProxy(self.testUserUUID, "POST",
                                      "/api/v3/user/sleep",
                                      {'data': False})

      ret_profile = proxy.habiticaProxy(self.testUserUUID, "GET",
                                        "/api/v3/user", None)
      ret_json = ret_profile.json()
      # User should not be sleeping
      self.assertEqual(ret_json['data']['preferences']['sleep'], False)

def randomGen():
    alphabet = "abcdefghijklmnopqrstuvwxyz0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ"
    length = 5
    string = ""
    for i in range(length):
      next_index = random.randrange(len(alphabet))
      string = string + alphabet[next_index]
    return string

if __name__ == '__main__':
    import emission.tests.common as etc

    etc.configLogging()
    unittest.main()
