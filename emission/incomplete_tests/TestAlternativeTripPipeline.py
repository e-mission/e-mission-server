from __future__ import unicode_literals
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import
# Standard imports
from future import standard_library
standard_library.install_aliases()
from builtins import next
from builtins import *
import unittest
import traceback
import json
import logging
import re
import datetime as pydt

# Our imports
import emission.analysis.modelling.user_model.alternative_trips_module as pipeline_module
from emission.analysis.modelling.user_model.alternative_trips_pipeline import AlternativeTripsPipeline
import emission.core.wrapper.trip_old as ecwt
from emission.analysis.modelling.user_model import query
from emission.core.wrapper.user import User
from emission.core.wrapper.client import Client
from emission.net.ext_service.moves import collect
# from emission.net.ext_services.gmaps.common import *
import emission.core.get_database as edb
from crontab import CronTab
import emission.tests.common as etc

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
    self.ModesColl = edb.get_mode_db()
    self.ModesColl.remove()
    edb.get_trip_db().remove()
    edb.get_section_db().remove()
    edb.get_alternatives_db().remove()

    self.assertEquals(self.ModesColl.estimated_document_count(), 0)

    dataJSON = json.load(open("emission/tests/data/modes.json"))
    for row in dataJSON:
      self.ModesColl.insert(row)
    
    # register each of the users and add sample trips to each user
    result = self.loadTestJSON("emission/tests/data/missing_trip")
    collect.processResult(self.testUUID, result)
    for trip in edb.get_trip_db().find():
        trip['trip_start_datetime'] = pydt.datetime.now() + pydt.timedelta(hours=-5)
        trip['trip_end_datetime'] = pydt.datetime.now()
        edb.get_trip_db().update({"_id": trip["_id"]}, trip)

    for section in edb.get_section_db().find():
        section['section_start_datetime'] = pydt.datetime.now() + pydt.timedelta(hours=-5)
        section['section_end_datetime'] = pydt.datetime.now()
        edb.get_section_db().update({"_id": section["_id"]}, section)
    
    self.pipeline = AlternativeTripsPipeline()

  def tearDown(self):
    edb.get_section_db().remove({"user_id": self.testUUID})
    self.ModesColl.remove()
    self.assertEquals(self.ModesColl.estimated_document_count(), 0)

  def loadTestJSON(self, fileName):
    fileHandle = open(fileName)
    return json.load(fileHandle)
    
  def testRetrieveAllUserTrips(self):
    #updated to 15 since filtering places
    trip_list = self.pipeline.get_trips_for_alternatives(self.testUUID)
    # self.assertEquals(len(list(trip_list)), 5) 
    
    # Trip 20140407T175709-0700 has two sections

#   def testScheduleAlternativeTrips(self):
#     user_crontab = CronTab(user=True)
#     user_crontab.remove_all()
#     user_crontab.write()
# 
#     trip_iter = self.pipeline.get_trips_for_alternatives(self.testUUID)
#     self.assertTrue(hasattr(trip_iter, '__iter__'))
#     trip_list = list(trip_iter)
#     firstElement = trip_list[0]
#     self.assertTrue(isinstance(firstElement, ecwt.E_Mission_Trip))
#     # calc_alternative_trips merely schedules the alternative trip calculation at a later time
#     # it can't return the alternative trips right now
#     pipeline_module.calc_alternative_trips(trip_list, immediate=False)
#     for trip in trip_list:
#         self.assertEqual(trip.pipelineFlags.alternativesStarted, True)
# 
#     # Re-open the crontab to see the new entries
#     user_crontab = CronTab(user=True)
#     jobs = [job for job in user_crontab]
#     self.assertEqual(len(jobs), len(trip_list))
#     self.assertEqual(jobs[0].hour, firstElement.start_time.hour)

  def testQueryAndSaveAlternatives(self):
    trip_iter = self.pipeline.get_trips_for_alternatives(self.testUUID)
    self.assertTrue(hasattr(trip_iter, '__iter__'))
    trip_list = list(trip_iter)
    for trip in trip_list:
        query.obtain_alternatives(trip.trip_id, self.testUUID)
    firstElement = trip_list[0]
    self.assertTrue(isinstance(firstElement, ecwt.E_Mission_Trip))

    for trip in trip_list:
        alt_it = pipeline_module.get_alternative_for_trip(trip)
        alt_list = list(alt_it)
        # TODO: Figure out why we sometimes have three alternatives and sometimes have 4.
        # We are querying for 4 alternatives in the code, so why don't we have all four
        # self.assertTrue(len(alt_list) == 3 or len(alt_list) == 4)

  '''
  def test_initialize_empty_perturbed_trips(self):
    db = edb.get_section_db()
    i = 0
    temp = db.find_one({'type' : 'move'})
    _id = temp['_id']
    #self.assertEquals(type(our_id), str)
    p_db = edb.get_perturbed_trips_db()
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
    json_trip = self.loadTestJSON("emission/tests/data/testModeInferFile")
    #self.assertEquals(type(json_trip), json)
    #json_trip = json_trip.read()
    trip = ecwt.E_Mission_Trip(json_trip[0])
    db = edb.get_section_db()
    pdb = edb.get_perturbed_trips_db()
    temp = db.find_one({'type' : 'move'})
    our_id = temp['_id']
    initialize_empty_perturbed_trips(our_id, pdb)
    update_perturbations(our_id, trip)
  '''
  def test_pipeline_e2e(self):
    self.pipeline.runPipeline()
    
  def testAlternativeTripStore(self):
    trip_list = self.pipeline.get_trips_for_alternatives(self.testUUID) 
    first_trip = next(trip_list)
    self.assertEquals(type(first_trip), ecwt.E_Mission_Trip)
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
      altTripsDB = edb.get_alternative_trips_db()
      json_trip = altTripsDB.find_one({"type" : "move"})
      self.assertFalse("alalalalal")
      self.assertTrue(json_trip)
   '''

if __name__ == '__main__':
  etc.configLogging()
  unittest.main()
