# Standard imports
import unittest
import logging
import json
import uuid
import attrdict as ad
import random

# Our imports
import emission.net.ext_service.habitica.register as reg
import emission.core.get_database as edb

class TestHabiticaRegister(unittest.TestCase):
  def setUp(self):
    print "Test setup called"
    self.testUserUUID = uuid.uuid4()
    autogen_string = randomGen()
    autogen_email = autogen_string + '@test.com'
    self.sampleAuthMessage1 = {'username': autogen_string, 'email': autogen_email, 
      'password': autogen_string, 'our_uuid': self.testUserUUID}

  def testAddNewUser(self):
    sampleAuthMessage1Ad = ad.AttrDict(self.sampleAuthMessage1)
    reg.habiticaRegister(sampleAuthMessage1Ad.username, sampleAuthMessage1Ad.email, 
      sampleAuthMessage1Ad.password, sampleAuthMessage1Ad.our_uuid)

    find_it = edb.get_habitica_db().find({"user_id": self.testUserUUID})
    self.assertEqual(find_it.count(), 1)
    
    user_val = list(find_it)[0]
    self.assertIsNotNone(user_val['habitica_id'])
    self.assertEqual(user_val['habitica_username'], sampleAuthMessage1Ad.username)
  

def randomGen():
    alphabet = "abcdefghijklmnopqrstuvwxyz0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ"
    length = 5
    string = ""
    for i in range(length):
      next_index = random.randrange(len(alphabet))
      string = string + alphabet[next_index]
    return string

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    unittest.main()
