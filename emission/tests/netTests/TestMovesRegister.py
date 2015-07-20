import unittest
import json
import sys
import os
import uuid

sys.path.append("%s" % os.getcwd())
from main import auth

class TestAuthFunctions(unittest.TestCase):
  def setUp(self):
    print "Test setup called"
    self.testUserUUID = uuid.uuid4()
    self.sampleAuthMessage1 = {'user_id': 99999999999999999, 'access_token': 'Initial_token', 'expires_in': 15551999, 'token_type': 'bearer', 'refresh_token': 'Initial_refresh_token'}
    self.sampleAuthMessage2 = {'user_id': 99999999999999999, 'access_token': 'Updated_token', 'expires_in': 15551999, 'token_type': 'bearer', 'refresh_token': 'Updated_refresh_token'}
    self.sampleAuthMessage3 = {'user_id': 11111111111111111, 'access_token': 'Updated_token', 'expires_in': 15551999, 'token_type': 'bearer', 'refresh_token': 'Updated_refresh_token'}

  def tearDown(self):
    print "Test teardown called"
    auth.deleteAllTokens(self.testUserUUID)

  # One part of this is the communication with moves. That is an integration test, not a unit test,
  # so we don't test that here. Instead, we test the save and retrieval of the datastructure 
  # that we get back from moves. Some sample auth messages are shown above

  # TODO: See if we can use Mock to create a mock Moves server that can allow
  # us to integration test as well.
  def testAddNewUser(self):
    auth.saveAccessToken(self.sampleAuthMessage1, self.testUserUUID)
    savedTokens = auth.getAccessToken(self.testUserUUID)
    self.assertEqual(len(savedTokens), 1)
  
    # These are the values that are set in getAccessToken
    self.assertEqual(savedTokens[0]["our_uuid"], self.testUserUUID)
    self.assertEqual(savedTokens[0]["_id"], 99999999999999999)

    self.assertEqual(savedTokens[0]["access_token"], "Initial_token")
    self.assertEqual(savedTokens[0]["refresh_token"], "Initial_refresh_token")

  def testUpdateExistingUser(self):
    auth.saveAccessToken(self.sampleAuthMessage1, self.testUserUUID)
    auth.saveAccessToken(self.sampleAuthMessage2, self.testUserUUID)
    savedTokens = auth.getAccessToken(self.testUserUUID)
    self.assertEqual(len(savedTokens), 1)

    # You can print out the current state for further reference
    print savedTokens[0]

    # These identify the user and should not be changed
    self.assertEqual(savedTokens[0]["our_uuid"], self.testUserUUID)
    self.assertEqual(savedTokens[0]["_id"], 99999999999999999)

    # These will be sent fresh with a second call to linkToMoves and should be changed
    self.assertEqual(savedTokens[0]["access_token"], "Updated_token")
    self.assertEqual(savedTokens[0]["refresh_token"], "Updated_refresh_token")

  def testDeleteEntries(self):
    testUser2 = uuid.uuid4()
    auth.saveAccessToken(self.sampleAuthMessage1, self.testUserUUID)
    auth.saveAccessToken(self.sampleAuthMessage3, testUser2)
    savedTokens = auth.getAccessToken(self.testUserUUID)
    self.assertEqual(len(savedTokens), 1)

    savedTokens2 = auth.getAccessToken(testUser2)
    self.assertEqual(len(savedTokens2), 1)

    auth.deleteAllTokens(self.testUserUUID)
    savedTokens = auth.getAccessToken(self.testUserUUID)
    self.assertEqual(len(savedTokens), 0)

    savedTokens2 = auth.getAccessToken(testUser2)
    self.assertEqual(len(savedTokens2), 1)

  # This is the case where the same user is linked to moves using two separate devices
  # This is not really a supported configuration, but we don't explicitly prevent it
  # So let's make sure that we don't crash something inadvertently
  def testDoubleRegisteredUser(self):
    auth.saveAccessToken(self.sampleAuthMessage1, self.testUserUUID)
    auth.saveAccessToken(self.sampleAuthMessage3, self.testUserUUID)
    savedTokens = auth.getAccessToken(self.testUserUUID)
    self.assertEqual(len(savedTokens), 2)

    firstDevice = None
    secondDevice = None

    for token in savedTokens:
      if token["_id"] == 99999999999999999:
        firstDevice = token
      else:
        secondDevice = token

    self.assertEqual(firstDevice["access_token"], "Initial_token")
    self.assertEqual(firstDevice["refresh_token"], "Initial_refresh_token")

    self.assertEqual(secondDevice["_id"], 11111111111111111)
    self.assertEqual(secondDevice["access_token"], "Updated_token")
    self.assertEqual(secondDevice["refresh_token"], "Updated_refresh_token")

if __name__ == '__main__':
    unittest.main()
