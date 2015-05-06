import unittest
import json
#from utils import load_database_json, purge_database_json
#from main import tripManager
from pymongo import MongoClient
import logging
from get_database import get_db, get_mode_db, get_section_db, get_trip_db
import re
import sys 
import os
from datetime import datetime, timedelta
from recommender.utility_model_pipeline import UtilityModelPipeline
# Needed to modify the pythonpath
sys.path.append("%s/../CFC_WebApp/" % os.getcwd())
sys.path.append("%s" % os.getcwd())
from dao.user import User
from dao.client import Client
import tests.common
from moves import collect
from recommender.user_utility_model import UserUtilityModel

logging.basicConfig(level=logging.DEBUG)

class TestUtilityModelPipeline(unittest.TestCase):
  def setUp(self):
    self.testUUID = "myuuidisverylongandcomplicated"
    #self.testUserEmails = ["test@example.com", "best@example.com", "fest@example.com",
    #                       "rest@example.com", "nest@example.com"]
    self.serverName = 'localhost'

    self.testUsers = []

    #for userEmail in self.testUserEmails:
    #  User.register(userEmail)
    #  self.testUsers += [User.fromEmail(section['user_id'])] # can access uuid with .uuid

    # Sometimes, we may have entries left behind in the database if one of the tests failed
    # or threw an exception, so let us start by cleaning up all entries
    self.ModesColl = get_mode_db()
    self.ModesColl.remove()

    self.assertEquals(self.ModesColl.find().count(), 0)

    dataJSON = json.load(open("tests/data/modes.json"))
    for row in dataJSON:
      self.ModesColl.insert(row)
    
    #TODO: add many trip filter functions to play with
    self.trip_filters = None

    result = self.loadTestJSON("tests/data/missing_trip")
    collect.processResult(self.testUUID, result)
    self.pipeline = UtilityModelPipeline()

  def tearDown(self):
    get_section_db().remove({"user_id": self.testUUID})
    self.ModesColl.remove()
    self.assertEquals(self.ModesColl.find().count(), 0)
    #for testUser in self.testUsersEmails:
    #  purge_database_json.purgeData('localhost', testUser)

  def loadTestJSON(self, fileName):
    fileHandle = open(fileName)
    return json.load(fileHandle)
    
  def testRetrieveTrainingTrips(self):
    #now 15 since filtering places
    trip_list = self.pipeline.get_training_trips(self.testUUID)
    # self.assertEquals(len(list(trip_list)), 5) 

  def testBuildUserModel(self):
    #get a users trips, there should be 21
    trip_list = self.pipeline.get_training_trips(self.testUUID)
    model = self.pipeline.build_user_model(self.testUUID, trip_list)
    print "model is a %s" % (type(model))
    self.assertTrue(isinstance(model, UserUtilityModel)), 

  '''
  #Modifying the user model is a recommendation pipeline task: TO DELETE
  def testModifyUserModel(self):
    trip_list = self.pipeline.get_training_trips(self.testUUID)
    model = self.pipeline.build_user_model(self.testUUID, trip_list)
    print model
    self.assertTrue(isinstance(model, UserUtilityModel))
    new_model = self.pipeline.modify_user_utility_model(model)
    self.assertTrue(isinstance(model, UserUtilityModel))
    self.assertNotEquals(new_model, model)
  '''

  def test_pipeline_e2e(self):
    self.pipeline.runPipeline()

if __name__ == '__main__':
    unittest.main()
