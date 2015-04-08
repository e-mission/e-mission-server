import unittest
import json
#from utils import load_database_json, purge_database_json
#from main import tripManager
from pymongo import MongoClient
import logging
from get_database import *
import re
import sys 
import os
from datetime import datetime, timedelta
import recommender.alternative_trips_module as pipeline_module
import recommender.alternative_trips_pipeline as pipeline 
from recommender.trip import *
# Needed to modify the pythonpath
sys.path.append("%s/../CFC_WebApp/" % os.getcwd())
sys.path.append("%s" % os.getcwd())
from dao.user import User
from dao.client import Client
import tests.common
from moves import collect
from recommender.common import *

logging.basicConfig(level=logging.DEBUG)

class TestAlternativeTripPipeline(unittest.TestCase):
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

    # import data from tests/data/testModeInferFiles
    #self.pipeline = pipeline.ModeRecommendationPipeline()
    #self.testRecommendationPipeline()
    # register each of the users and add sample trips to each user
    result = self.loadTestJSON("tests/data/missing_trip")
    collect.processResult(self.testUUID, result)

  def tearDown(self):
    get_section_db().remove({"user_id": self.testUUID})
    self.ModesColl.remove()
    self.assertEquals(self.ModesColl.find().count(), 0)
    #for testUser in self.testUsersEmails:
    #  purge_database_json.purgeData('localhost', testUser)

  def loadTestJSON(self, fileName):
    fileHandle = open(fileName)
    return json.load(fileHandle)
    
  def testRetrieveAllUserTrips(self):
    #get a users trips, there should be 21
    trip_list = pipeline.get_trips_for_alternatives(self.testUUID)
    self.assertEquals(len(list(trip_list)), 21) 
    # Trip 20140407T175709-0700 has two sections

  def testAugmentTrips(self):
    trip_list = pipeline.get_trips_for_alternatives(self.testUUID)
    self.assertTrue(hasattr(trip_list, '__iter__'))
    # TODO: Why should this not be 21? Check with Shaun?
    # self.assertNotEquals(len(trip_list), 21) 
    firstElement = list(trip_list)[0]
    self.assertTrue(isinstance(firstElement, E_Mission_Trip))
    # calc_alternative_trips merely schedules the alternative trip calculation at a later time
    # it can't return the alternative trips right now
    # TODO: Figure out how to get this to work
    pipeline_module.calc_alternative_trips([firstElement].__iter__())
    # self.assertEquals(type(alternative_list), list)

  def test_initialize_empty_perturbed_trips(self):
    db = get_section_db()
    i = 0
    temp = db.find_one({'type' : 'move'})
    _id = temp['_id']
    #self.assertEquals(type(our_id), str)
    p_db = get_perturbed_trips_db()
    initialize_empty_perturbed_trips(_id, p_db)
    our_id = _id.replace('.', '')
    temp = p_db.find_one({"our_id" : our_id})
    self.assertEquals(type(temp), dict)
    # for x in db.find({'type' : 'move'}):
    #     if x['_id'] != _id:
    #         new_id = x['_id']
    # initialize_empty_perturbed_trips(new_id, p_db)
    # new_id = new_id.replace('.', '')
    # temp = p_db.find_one({"_id" : _id})
    # self.assertEquals(type(temp), dict)

  def test_update_perturbations(self):
    json_trip = self.loadTestJSON("tests/data/testModeInferFile")
    #self.assertEquals(type(json_trip), json)
    #json_trip = json_trip.read()
    trip = E_Mission_Trip(json_trip[0])
    db = get_section_db()
    pdb = get_perturbed_trips_db()
    temp = db.find_one({'type' : 'move'})
    our_id = temp['_id']
    initialize_empty_perturbed_trips(our_id, pdb)
    update_perturbations(our_id, trip)
    

   	
  def storeAlternativeTrips(self):
    trip_list = pipeline.get_user_trips(self.testUUID, self.trip_filters)
    self.assertEquals(type(trip_list), list)
    self.assertNotEquals(len(trip_list), 21) 
    self.assertEquals(type(trip_list[0]), E_Mission_Trip)
    alternative_list = pipeline.get_alternative_trips(self.testUUID, trip_list[0]._id)
    pipeline.store_alternative_trips(alternative_list)
    self.assertEquals(type(alternative_list), list)


  # def testLoadDatabse(self):
  #   trip_list = pipeline.get_user_trips(self.testUUID, self.trip_filters)
  #   alternative_list = pipeline.get_alternative_trips(self.testUUID, trip_list[0]._id)
  #   pipeline.store_alternative_trips(alternative_list)
  #   altTripsDB = get_alternative_trips_db()
  #   json_trip = altTripsDB.find_one({"type" : "move"})
  #   self.assertTrue(json_trip)



if __name__ == '__main__':
    unittest.main()
