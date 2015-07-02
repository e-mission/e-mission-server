import unittest
import traceback
import json
#from utils import load_database_json, purge_database_json
#from main import tripManager
from pymongo import MongoClient
import logging
from get_database import *
import re
import sys 
import os
import datetime as pydt
import recommender.alternative_trips_module as pipeline_module
from recommender.alternative_trips_pipeline import AlternativeTripsPipeline
from recommender.trip import *
from recommender import query
# Needed to modify the pythonpath
sys.path.append("%s/../CFC_WebApp/" % os.getcwd())
sys.path.append("%s" % os.getcwd())
from dao.user import User
from dao.client import Client
import tests.common
from moves import collect
from recommender.common import *
import collections
from crontab import CronTab
from trip_generator.fake_trip import create_fake_trips

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
    get_trip_db().remove()
    get_section_db().remove()
    get_alternatives_db().remove()

    self.assertEquals(self.ModesColl.find().count(), 0)

    dataJSON = json.load(open("tests/data/modes.json"))
    for row in dataJSON:
      self.ModesColl.insert(row)
    
    # register each of the users and add sample trips to each user
    # result = self.loadTestJSON("tests/data/missing_trip")
    # collect.processResult(self.testUUID, result)


    for trip in get_trip_db().find():
        trip['trip_start_datetime'] = pydt.datetime.now() + pydt.timedelta(hours=-5)
        trip['trip_end_datetime'] = pydt.datetime.now()
        get_trip_db().update({"_id": trip["_id"]}, trip)

    for section in get_section_db().find():
        section['section_start_datetime'] = pydt.datetime.now() + pydt.timedelta(hours=-5)
        section['section_end_datetime'] = pydt.datetime.now()
        get_section_db().update({"_id": section["_id"]}, section)
    
    self.pipeline = AlternativeTripsPipeline()

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
    #updated to 15 since filtering places
    trip_list = self.pipeline.get_trips_for_alternatives(self.testUUID)
    # self.assertEquals(len(list(trip_list)), 5) 
    
    # Trip 20140407T175709-0700 has two sections

  def testScheduleAlternativeTrips(self):
    user_crontab = CronTab(user=True)
    user_crontab.remove_all()
    user_crontab.write()

    trip_iter = self.pipeline.get_trips_for_alternatives(self.testUUID)
    self.assertTrue(hasattr(trip_iter, '__iter__'))
    trip_list = list(trip_iter)
    firstElement = trip_list[0]
    self.assertTrue(isinstance(firstElement, E_Mission_Trip))
    # calc_alternative_trips merely schedules the alternative trip calculation at a later time
    # it can't return the alternative trips right now
    pipeline_module.calc_alternative_trips(trip_list, immediate=False)
    for trip in trip_list:
        self.assertEqual(trip.pipelineFlags.alternativesStarted, True)

    # Re-open the crontab to see the new entries
    user_crontab = CronTab(user=True)
    jobs = [job for job in user_crontab]
    self.assertEqual(len(jobs), len(trip_list))
    self.assertEqual(jobs[0].hour, firstElement.start_time.hour)

  def testQueryAndSaveAlternatives(self):
    trip_iter = self.pipeline.get_trips_for_alternatives(self.testUUID)
    self.assertTrue(hasattr(trip_iter, '__iter__'))
    trip_list = list(trip_iter)
    for trip in trip_list:
        query.obtain_alternatives(trip.trip_id, self.testUUID)
    firstElement = trip_list[0]
    self.assertTrue(isinstance(firstElement, E_Mission_Trip))

    for trip in trip_list:
        alt_it = pipeline_module.get_alternative_for_trip(trip)
        alt_list = list(alt_it)
        # TODO: Figure out why we sometimes have three alternatives and sometimes have 4.
        # We are querying for 4 alternatives in the code, so why don't we have all four
        self.assertTrue(len(alt_list) == 3 or len(alt_list) == 4)

  '''
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
  '''
  def test_pipeline_e2e(self):
    self.pipeline.runPipeline()
    
  def testAlternativeTripStore(self):
    trip_list = self.pipeline.get_trips_for_alternatives(self.testUUID) 
    first_trip = trip_list.next()
    self.assertEquals(type(first_trip), E_Mission_Trip)
    # alternative_list = pipeline_module.get_alternative_trips(trip_list)
    # for alt in alternative_list:
    #     if alt:
    #         alt.store_to_db()
    #self.assertGreater(len(list(alternative_list)), 0)
  '''
  def testLoadDatabse(self):
      trip_list = self.pipeline.get_trips_for_alternatives(self.testUUID)
      alternative_list = pipeline_module.get_alternative_trips(trip_list.next()._id)
      pipeline.store_alternative_trips(alternative_list)
      altTripsDB = get_alternative_trips_db()
      json_trip = altTripsDB.find_one({"type" : "move"})
      self.assertFalse("alalalalal")
      self.assertTrue(json_trip)
   '''

if __name__ == '__main__':
    unittest.main()
