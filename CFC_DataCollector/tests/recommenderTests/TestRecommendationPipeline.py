import unittest
import json
#from utils import load_database_json, purge_database_json
#from main import tripManager
from pymongo import MongoClient
import logging
from get_database import get_db, get_mode_db, get_section_db, get_trip_db, get_routeCluster_db
import re
import sys
import os
from datetime import datetime, timedelta
from recommender.recommendation_pipeline import RecommendationPipeline
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

    get_section_db().remove({"user_id": self.testUUID})
    result = self.loadTestJSON("tests/data/missing_trip")
    collect.processResult(self.testUUID, result)
    print get_section_db().find().count()
    self.pipeline = RecommendationPipeline()

  def tearDown(self):
    get_section_db().remove({"user_id": self.testUUID})
    self.ModesColl.remove()
    get_routeCluster_db().remove()
    self.assertEquals(self.ModesColl.find().count(), 0)

  def loadTestJSON(self, fileName):
    fileHandle = open(fileName)
    return json.load(fileHandle)

  def testRetrieveTripsToImprove(self):
    #updated to 15, since I am filtering out places
    trip_list = list(self.pipeline.get_trips_to_improve(self.testUUID))
    self.assertEquals(len(trip_list), 0)

  def testRetrieveTripsToImproveWithClusters(self):
    sectionList = list(get_section_db().find())
    get_routeCluster_db().insert({"user": self.testUUID,
        "clusters": 
            {"cluster1": [s["_id"] for s in sectionList[0:10]],
             "cluster2": [s["_id"] for s in sectionList[10:20]]}})
    trip_list = list(self.pipeline.get_trips_to_improve(self.testUUID))
    self.assertEquals(len(trip_list), 2)

  def testRecommendTrip(self):
    recommended_trips = self.pipeline.runPipeline()
    #trip_list = self.pipeline.get_trips_to_improve(self.testUUID)
    #utility_model = self.pipeline.get_selected_user_utility_model(self.testUUID)
    #recommended_trips = self.pipeline.recommend_trips(trip_list[0]._id, utility_model)

  def testCanonical(self):
    canonical_trip_iter = TripIterator(self.testUUID, ['trips', 'get_canonical']).__iter__()

  def test_pipeline_e2e(self):
    self.pipeline.runPipeline()

if __name__ == '__main__':
    unittest.main()
