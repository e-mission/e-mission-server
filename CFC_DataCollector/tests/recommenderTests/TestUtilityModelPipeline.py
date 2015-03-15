import unittest
import json
import logging
import numpy as np
from get_database import get_db, get_mode_db, get_section_db
from recommender import pipeline
import sys
import os

logging.basicConfig(level=logging.DEBUG)

class TestPipeline(unittest.TestCase):
  def setUp(self):
  	self.testUserEmails = ["test@example.com", "best@example.com", "fest@example.com",
                      "rest@example.com", "nest@example.com"]
    self.serverName = 'localhost'

    self.testUsers = []
    for userEmail in self.testUserEmails:
      User.register(userEmail)
      self.testUsers += [User.fromEmail(section['user_id'])] # can access uuid with .uuid

    # import data from tests/data/testModeInferFiles

    self.pipeline = pipeline.ModeRecommendationPipeline()
    self.testRecommendationPipeline()
    # register each of the users and add sample trips to each user

  def tearDown(self):
  	for testUser in self.testUsersEmails:
      purge_database_json.purgeData('localhost', testUser)

  def testTargetTripsPipeline(self):
  	self.testTargetTrips = [pipeline.getTargetTrips(testUser.uuid) for testUser in testUsers]
  	# series of assertions to check output

  def testAugmentTripsPipeline(self):
  	# call functions in the augment_trips_pipeline file using trips from self.testTargetTrips
  	# assertions to check various characteristics of output
   	
  def testUtilityModelPipeline(self):
  	# call functions from utility_model_pipeline.py and using augmented trips from testAugmentTripsPipeline and self.testUsers
  	# assertions to check various characteristics of output

  def testRecommendationPipeline(self):
  	self.testTripsToImprovePipeline();
    self.testUtilityModelPipeline();
    self.testRecommendationPipeline();
    # assertions to check various characteristcs of pipeline overall

if __name__ == '__main__':
    unittest.main()