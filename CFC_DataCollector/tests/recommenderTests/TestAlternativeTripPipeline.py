import unittest
import json
import logging
import numpy as np
from get_database import get_db, get_mode_db, get_section_db
from recommender import pipeline
import sys
import os

logging.basicConfig(level=logging.DEBUG)

class TestAlternativeTripPipeline(unittest.TestCase):
  def setUp(self):
    self.testUserEmails = ["test@example.com", "best@example.com", "fest@example.com",
                           "rest@example.com", "nest@example.com"]
    self.serverName = 'localhost'

    self.testUsers = []
    for userEmail in self.testUserEmails:
      User.register(userEmail)
      self.testUsers += [User.fromEmail(section['user_id'])] # can access uuid with .uuid

    # Sometimes, we may have entries left behind in the database if one of the tests failed
    # or threw an exception, so let us start by cleaning up all entries
    self.ModesColl = get_mode_db()
    self.ModesColl.remove()

    self.assertEquals(self.ModesColl.find().count(), 0)

    dataJSON = json.load(open("tests/data/modes.json"))
    for row in dataJSON:
      self.ModesColl.insert(row)
    
    #TODO: add many trip filter functions to play with
    self.trip_filter = None

    # import data from tests/data/testModeInferFiles
    #self.pipeline = pipeline.ModeRecommendationPipeline()
    #self.testRecommendationPipeline()
    # register each of the users and add sample trips to each user

  def tearDown(self):
    get_section_db().remove({"user_id": self.testUUID})
    self.ModesColl.remove()
    self.assertEquals(self.ModesColl.find().count(), 0)
    for testUser in self.testUsersEmails:
      purge_database_json.purgeData('localhost', testUser)

    
  def testRetrieveAllUserTrips(self):
    trip_list = get_user_trips(self.testUUID)  
    

  def testAugmentTrips(self):


  def testAugmentTripsPipeline(self):
  	# call functions in the augment_trips_pipeline file using trips from self.testTargetTrips
  	# assertions to check various characteristics of output
   	


if __name__ == '__main__':
    unittest.main()
