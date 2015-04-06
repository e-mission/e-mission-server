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
from recommender import recommendation_pipeline as pipeline
# Needed to modify the pythonpath
sys.path.append("%s/../CFC_WebApp/" % os.getcwd())
sys.path.append("%s" % os.getcwd())
from dao.user import User
from dao.client import Client
import tests.common
from moves import collect
from recommender.tripiterator import TripIterator

logging.basicConfig(level=logging.DEBUG)

class TestRecommendationPipeline(unittest.TestCase):
  def setUp(self):
    self.testUUID = "myuuidisverylongandcomplicated"
    self.serverName = 'localhost'
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

  def tearDown(self):
    get_section_db().remove({"user_id": self.testUUID})
    self.ModesColl.remove()
    self.assertEquals(self.ModesColl.find().count(), 0)

  def loadTestJSON(self, fileName):
    fileHandle = open(fileName)
    return json.load(fileHandle)

  def testRetrieveTripsToImprove(self):
    #get a users trips, there should be 21
    trip_list = pipeline.get_trips_to_improve(self.testUUID, self.trip_filters)
    self.assertEquals(len(trip_list), 22)
    # Trip 20140407T175709-0700 has two sections

  def testRecommendTrip(self):
    trip_list = pipeline.get_trips_to_improve(self.testUUID, self.trip_filters)
    utility_model = pipeline.get_user_utility_models(self.testUUID)
    recommended_trips = pipeline.recommend_trips(trip_list[0]._id, utility_model)

  def testCanonical(self):
    canonical_trip_iter = TripIterator(self.testUUID, ['trips', 'get_canonical']).__iter__()

if __name__ == '__main__':
    unittest.main()
