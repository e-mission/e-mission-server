from __future__ import print_function
from __future__ import unicode_literals
from __future__ import division
from __future__ import absolute_import
# Standard imports
from future import standard_library
standard_library.install_aliases()
from builtins import *
import unittest
import json
from pymongo import MongoClient
import logging
from datetime import datetime, timedelta

# Our imports
from emission.core.get_database import get_mode_db, get_section_db, get_trip_db, get_routeCluster_db
from emission.analysis.result.recommendation.recommendation_pipeline import RecommendationPipeline
from emission.core.wrapper.user import User
from emission.core.wrapper.client import Client
from emission.net.ext_service.moves import collect
from emission.core.wrapper.tripiterator import TripIterator

import emission.tests.common as etc

class TestRecommendationPipeline(unittest.TestCase):
  def setUp(self):
    self.testUUID = "myuuidisverylongandcomplicated"
    self.serverName = 'localhost'
    # Sometimes, we may have entries left behind in the database if one of the tests failed
    # or threw an exception, so let us start by cleaning up all entries
    self.ModesColl = get_mode_db()
    self.ModesColl.remove()
    self.assertEquals(self.ModesColl.find().count(), 0)

    dataJSON = json.load(open("emission/tests/data/modes.json"))
    for row in dataJSON:
      self.ModesColl.insert(row)

    get_section_db().remove({"user_id": self.testUUID})
    result = self.loadTestJSON("emission/tests/data/missing_trip")
    collect.processResult(self.testUUID, result)
    print(get_section_db().find().count())
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
    etc.configLogging()
    unittest.main()
