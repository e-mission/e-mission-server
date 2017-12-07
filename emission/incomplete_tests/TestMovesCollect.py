from __future__ import unicode_literals
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import
# Standard imports
from future import standard_library
standard_library.install_aliases()
from builtins import *
import unittest
import logging
import json
import re

# Our imports
import emission.tests.common
from emission.core.get_database import get_mode_db, get_section_db, get_trip_db
from emission.net.ext_service.moves import collect

class TestMovesCollect(unittest.TestCase):
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

  def tearDown(self):
    get_section_db().remove({"user_id": self.testUUID})
    self.ModesColl.remove()
    self.assertEquals(self.ModesColl.find().count(), 0)

  def loadTestJSON(self, fileName):
    fileHandle = open(fileName)
    return json.load(fileHandle)

  # Would be good to avoid duplicating this, change loadTestJSON to send in set
  # of additional preprocessing steps?
  def loadReplaceUser(self, fileName, origEmail, fakeEmail):
    fileHandle = open(fileName)
    dataStr = fileHandle.readline()
    dataStr = emission.tests.common.fixFormat(dataStr)
    dataStr = dataStr.replace(origEmail, fakeEmail)
    dataJSON = json.loads(dataStr)
    return dataJSON

  # This is a trip where we missed a cycling trip section. I recieved consent
  # to check this trip into the repository on Tuesday, Apr 8th, at 7:40am
  def testMissingSections(self):
    result = self.loadTestJSON("emission/tests/data/missing_trip")
    collect.processResult(self.testUUID, result)

    SectionColl = get_section_db()
    storedSections = SectionColl.find({'user_id': self.testUUID})
    self.assertEquals(storedSections.count(), 21)
    
    tripToWorkSections = SectionColl.find({'$and' : [{'user_id': self.testUUID,
                                                      'trip_id': '20140407T085210-0700'}]})
    self.assertEquals(tripToWorkSections.count(), 5)
    
     
  def testDoubleLoad(self):
    result = self.loadTestJSON("emission/tests/data/missing_trip")
    collect.processResult(self.testUUID, result)
    collect.processResult(self.testUUID, result)

    SectionColl = get_section_db()
    storedSections = SectionColl.find({'user_id': self.testUUID})
    self.assertEquals(storedSections.count(), 21)

    tripToWorkSections = SectionColl.find({'$and' : [{'user_id': self.testUUID,
                                                      'trip_id': '20140407T085210-0700'}]})
    self.assertEquals(tripToWorkSections.count(), 5)

  def testPartialLoadUpdate(self):
    resultAll = self.loadTestJSON("emission/tests/data/missing_trip")
    resultSubset = self.loadTestJSON("emission/tests/data/missing_trip_subset")
    collect.processResult(self.testUUID, resultSubset)

    SectionColl = get_section_db()
    tripToWorkSections = SectionColl.find({'$and' : [{'user_id': self.testUUID,
                                                      'trip_id': '20140407T085210-0700'}]})
    self.assertEquals(tripToWorkSections.count(), 3)

    collect.processResult(self.testUUID, resultAll)
    tripToWorkSections = SectionColl.find({'$and' : [{'user_id': self.testUUID,
                                                      'trip_id': '20140407T085210-0700'}]})
    self.assertEquals(tripToWorkSections.count(), 5)

  def testBlankToday(self):
    result = self.loadTestJSON("emission/tests/data/test2_blank_today")
    collect.processResult(self.testUUID, [result[0]])
    collect.processResult(self.testUUID, [result[1]])
    
    SectionColl = get_section_db()
    self.assertTrue(SectionColl.find({'user_id': self.testUUID}).count() > 0)

  def testWeirdLoadWithNoSections(self):
    result = self.loadTestJSON("emission/tests/data/test1_blank_today")
    collect.processResult(self.testUUID, result)

  def testYesterdayForJustSignedUp(self):
    result = self.loadTestJSON("emission/tests/data/yesterday_for_justsignedup")
    collect.processResult(self.testUUID, result)

    SectionColl = get_section_db()
    self.assertEquals(SectionColl.find({"user_id": self.testUUID}).count(), 0)

  def testPlaceLoad(self):
    result = self.loadTestJSON("emission/tests/data/test20140410")
    collect.processResult(self.testUUID, result)

    # Check that the trips are loaded correctly
    TripColl = get_trip_db()
    firstStoredTrip = TripColl.find_one({'$and': [{'user_id': self.testUUID,
                                      'trip_id': '20140409T191531-0700'}]})
    logging.debug("selected trip = %s" % firstStoredTrip)
    # For some reason, the place is always "unknown", at least for this set of test trips.
    # Maybe it is related to the fact that they haven't been tagged in FourSquare
    self.assertEqual(firstStoredTrip['type'], 'place')
    self.assertEqual(firstStoredTrip['trip_start_time'], '20140409T191531-0700')
    self.assertEqual(firstStoredTrip['trip_end_time'], "20140410T065227-0700")
    self.assertIn('place_location', firstStoredTrip['place'])
    self.assertEqual(firstStoredTrip['place']['place_location'], {'type': 'Point',
                                                            'coordinates': [-122.08632, 37.391]})

    # Now, check that we have the sections as well. The previous trip did not
    # have any sections. This one does
    tripWithSections = TripColl.find_one({'$and': [{'user_id': self.testUUID,
                                      'trip_id': '20140410T071320-0700'}]})

    self.assertNotEqual(tripWithSections, None)
    self.assertEqual(tripWithSections['sections'], [0])

    SectionColl = get_section_db()
    sectionForTrip = SectionColl.find_one({'$and': [{'user_id': self.testUUID,
                                      'trip_id': '20140410T071320-0700',
                                      'section_id': 0}]})
    self.assertNotEqual(sectionForTrip, None)

  def testUpdateSectionForExistingTrip(self):
    result = self.loadTestJSON("emission/tests/data/missing_trip")
    collect.processResult(self.testUUID, result)

    SectionColl = get_section_db()
    storedSections = SectionColl.find({'user_id': self.testUUID})
    self.assertEquals(storedSections.count(), 21)
    # Trip 20140407T175709-0700 has two sections
    storedTripSections = SectionColl.find({'$and': [{'user_id': self.testUUID},
                                                    {'trip_id': '20140407T175709-0700'}]})
    self.assertEquals(storedTripSections.count(), 2)

    TripColl = get_trip_db()
    storedTrips = TripColl.find({'$and': [{'user_id': self.testUUID},
                                                    {'trip_id': '20140407T175709-0700'}]})
    self.assertEquals(storedTrips.count(), 1)
    for trip in storedTrips:
      self.assertEquals(len(trip['sections']), 2)

    selTripFromMoves = None
    for i, seg in enumerate(result[0]['segments']):
      if seg['startTime'] == '20140407T175709-0700':
        selTripFromMoves = seg

    copiedTripSections = []
    for i, act in enumerate(selTripFromMoves['activities']):
      act['startTime'] = '20140407T18%s039-0700' % (i + 2)
      copiedTripSections.append(act)

    self.assertEquals(len(copiedTripSections), 2)
    [selTripFromMoves['activities'].append(act) for act in copiedTripSections]
    self.assertEquals(len(selTripFromMoves['activities']), 4)

    collect.processResult(self.testUUID, result)

    storedTripSections = SectionColl.find({'$and': [{'user_id': self.testUUID},
                                                    {'trip_id': '20140407T175709-0700'}]})
    self.assertEquals(storedTripSections.count(), 4)

    storedTrips = TripColl.find({'$and': [{'user_id': self.testUUID},
                                                    {'trip_id': '20140407T175709-0700'}]})
    self.assertEquals(storedTrips.count(), 1)

    # This is actually a bug in the existing code. Need to fix it.
    for trip in storedTrips:
      self.assertEquals(len(trip['sections']), 2)

  def testFillSectionWithValidData(self):
    testMovesSec = {}
    testMovesSec['manual'] = True
    testMovesSec['startTime'] = "20140407T183039-0700"
    testMovesSec['endTime'] = "20140407T191539-0700"
    testMovesSec['duration'] = 45
    testMovesSec['distance'] = 10
    testMovesSec['trackPoints'] = [{u'lat': 37, u'lon': -122, u'time': u'20140407T083200-0700'},
                                   {u'lat': 38, u'lon': -123, u'time': u'20140407T083220-0700'}]

    newSec = {} 
    collect.fillSectionWithMovesData(testMovesSec, newSec)

    self.assertEquals(newSec['manual'], True)
    self.assertEquals(newSec['section_start_time'], "20140407T183039-0700")
    self.assertEquals(newSec['section_end_time'], "20140407T191539-0700")
    self.assertEquals(newSec['section_start_datetime'].month, 0o4)
    self.assertEquals(newSec['section_end_datetime'].hour, 19)
    self.assertEquals(newSec['duration'], 45)
    self.assertEquals(newSec['distance'], 10)

    self.assertEquals(len(newSec['track_points']), 2)
    self.assertEquals(newSec['section_start_point']['coordinates'], [-122, 37])
    self.assertEquals(newSec['section_end_point']['coordinates'], [-123, 38])

  def testFillSectionWithInvalidData(self):
    testMovesSec = {}
    newSec = {} 
    collect.fillSectionWithMovesData(testMovesSec, newSec)

    self.assertEquals(newSec['manual'], None)

    # SHANKARI Unsure why this is '' instead of None. Will check with Mogeng on
    # it and then try to nomalize them
    self.assertEquals(newSec['section_start_time'], '')
    self.assertEquals(newSec['section_end_time'], '')
    # self.assertEquals(newSec['section_start_datetime'], )
    # self.assertEquals(newSec['section_end_datetime'], )
    self.assertEquals(newSec['duration'], None)
    self.assertEquals(newSec['distance'], None)

    self.assertEquals(len(newSec['track_points']), 0)
    self.assertEquals(newSec['section_start_point'], None)
    self.assertEquals(newSec['section_end_point'], None)

  def testFillTripWithValidData(self):
    testMovesSec = {}
    testMovesSec['type'] = 'move'
    testMovesSec['startTime'] = "20140407T183039-0700"
    testMovesSec['endTime'] = "20140407T191539-0700"
    testMovesSec['place'] = {}
    testMovesSec['place']['id'] = 10
    testMovesSec['place']['type'] = 'home'
    testMovesSec['place']['location'] = {u'lat': 37, u'lon': -122, u'time': u'20140407T083200-0700'}

    newSec = {} 
    collect.fillTripWithMovesData(testMovesSec, newSec)

    self.assertEquals(newSec['type'], 'move')
    self.assertEquals(newSec['trip_start_time'], "20140407T183039-0700")
    self.assertEquals(newSec['trip_end_time'], "20140407T191539-0700")
    self.assertEquals(newSec['trip_start_datetime'].month, 0o4)
    self.assertEquals(newSec['trip_end_datetime'].hour, 19)
    self.assertEquals(newSec['place']['place_location']['coordinates'][0], -122)

  def testFillTripWithInvalidData(self):
    testMovesSec = {}
    testMovesSec['type'] = 'move'
    testMovesSec['place'] = {}
    testMovesSec['place']['id'] = 10
    testMovesSec['place']['type'] = 'home'
    testMovesSec['place']['location'] = {u'lat': 37, u'lon': -122, u'time': u'20140407T083200-0700'}

    newSec = {} 
    collect.fillTripWithMovesData(testMovesSec, newSec)

    self.assertEquals(newSec['type'], 'move')
    self.assertEquals(newSec['trip_start_time'], "")
    self.assertEquals(newSec['trip_end_time'], "")
    self.assertEquals(newSec['trip_start_datetime'], None)
    self.assertEquals(newSec['trip_end_datetime'], None)
    self.assertEquals(newSec['place']['place_location']['coordinates'][0], -122)

  def testSectionFilterLabeling(self):
    """
    Tests that incoming section data parsed by collect.py is properly 
    labeled with a filter flag
    """
    # Testing first outer if statement in label_filtered
    testMovesSec = {}
    testMovesSec['manual'] = True
    testMovesSec['startTime'] = "20140407T183039-0700"
    testMovesSec['endTime'] = "20140407T191539-0700"
    testMovesSec['trackPoints'] = [{u'lat': 37, u'lon': -122, u'time': u'20140407T083200-0700'},
                                   {u'lat': 38, u'lon': -123, u'time': u'20140407T083220-0700'}]

    newSec = {'mode': ''} 
    collect.fillSectionWithMovesData(testMovesSec, newSec)
    collect.label_filtered_section(newSec)
    self.assertEquals(newSec['retained'], True)

    # Testing first outer elif statement in label_filtered
    testMovesSec['trackPoints'] = []
    newSec = {'mode':''} 
    collect.fillSectionWithMovesData(testMovesSec, newSec)
    collect.label_filtered_section(newSec)
    self.assertEquals(newSec['retained'], True)

    # Testing second outer elif statement in label_filtered
    testMovesSec['startTime'] = ""
    testMovesSec['endTime'] = ""
    testMovesSec['trackPoints'] = [{u'lat': 37, u'lon': -122, u'time': u'20140407T083200-0700'},
                                   {u'lat': 38, u'lon': -123, u'time': u'20140407T083220-0700'}]


    newSec = {'mode':''} 
    collect.fillSectionWithMovesData(testMovesSec, newSec)
    collect.label_filtered_section(newSec)
    self.assertEquals(newSec['retained'], True)

    # Testing outer else statement in label_filtered
    testMovesSec['startTime'] = ""
    testMovesSec['endTime'] = ""
    testMovesSec['trackPoints'] = ""
    
    newSec = {'mode':''} 
    collect.fillSectionWithMovesData(testMovesSec, newSec)
    collect.label_filtered_section(newSec)
    self.assertEquals(newSec['retained'], False)

if __name__ == '__main__':
    import emission.tests.common as etc

    etc.configLogging()
    unittest.main()
