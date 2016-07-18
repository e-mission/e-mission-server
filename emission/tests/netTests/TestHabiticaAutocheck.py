# Standard imports
import unittest
import logging
import json
import uuid
import attrdict as ad
import random

# Our imports
import emission.net.ext_service.habitica.proxy as proxy
import emission.net.ext_service.habitica.sync_habitica as autocheck
import emission.core.get_database as edb

class TestHabiticaRegister(unittest.TestCase):
  def setUp(self):
    #load test user
    #create habits "Bike" and "Walk"

  def testCreateExistingHabit(self):
    #try to create Bike
    #search this user's habits for the habit and check if there's exactly one

  def testCreateNewHabit(self):
    new_habit = randomGen()
    response = autocheck.create_habit(self, new_habit)
    #search this user's habits for the habit and check if there's exactly one

  def testAutomaticRewardActiveTransportation(self):
    #add 5000 to test user's bike distance (can create a fake trip, or can add manually)
    autocheck.reward_active_transportation(self.user_id) #fix self.user_id, get id properly
    #get user's habits and find "Bike"
    #check if the bike habit scored plus twice -- look at history



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
