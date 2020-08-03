from __future__ import unicode_literals
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import
# Test the class that supports usercache entries
# The main change here is that 

# Standard imports
from future import standard_library
standard_library.install_aliases()
from builtins import *
import logging
import unittest
from uuid import UUID
import geojson as gj
import bson.objectid as bo

# Our imports
import emission.core.wrapper.entry as ecwe
import emission.core.wrapper.motionactivity as ecwm
import emission.core.wrapper.trip as ecwt

import emission.tests.common as etc

class TestEntry(unittest.TestCase):
    def testWrapLocation(self):
        testEntryJSON = {'_id': '55a4418c7d65cb39ee9737cf',
         'data': {'accuracy': 52.5,
          'altitude': 0,
          'bearing': 0,
          'elapsedRealtimeNanos': 100623898000000,
          'latitude': 37.3885529,
          'longitude': -122.0879696,
          'loc': {"coordinates": [-122.0879696, 37.3885529], "type": "Point"},
          'sensed_speed': 0,
          'ts': 1436826356.852},
         'metadata': {'key': 'background/location',
          'platform': 'android',
          'read_ts': 0,
          'type': 'message',
          'write_ts': 1436826357.115,
          'write_fmt_time': '2015-07-13 15:25:57.115000-07:00'
        },
        'user_id': UUID('0763de67-f61e-3f5d-90e7-518e69793954')}

        entry = ecwe.Entry(testEntryJSON)
        self.assertEqual(entry.metadata.key, 'background/location')
        self.assertEqual(entry.metadata.type, 'message')
        self.assertEqual(entry.data.latitude, 37.3885529)
        self.assertEqual(entry.data.longitude, -122.0879696)
        # self.assertEqual(entry.data.loc, gj.Point((-122.0879696, 37.3885529)))
        self.assertTrue(isinstance(entry.data.loc, gj.Point))
        logging.debug("location time = %s, written at %s (%s)" % 
            (entry.data.ts, entry.metadata.write_ts, entry.metadata.write_fmt_time))

    def testWrapActivity(self):
        testEntryJSON = {
            '_id': '55a4418c7d65cb39ee9737d2',
                'data': {
                    'type': 5,
                    'confidence': 100,
                    'ts': 1436826360.493
                },
                'metadata': {'key': 'background/motion_activity',
                'platform': 'android',
                'read_ts': 0,
                'type': 'message',
                'write_ts': 1436826360.493,
                'write_fmt_time': '2015-07-13 15:26:00.493000-07:00'
            },
            'user_id': UUID('0763de67-f61e-3f5d-90e7-518e69793954')
        }
        entry = ecwe.Entry(testEntryJSON)
        self.assertEqual(entry.metadata.key, 'background/motion_activity')
        self.assertEqual(entry.metadata.type, 'message')
        self.assertEqual(entry.data.type, ecwm.MotionTypes.TILTING)
        self.assertEqual(entry.data.confidence, 100)
        logging.debug("activity time = %s, written at %s (%s)" % 
            (entry.data.ts, entry.metadata.write_ts, entry.metadata.write_fmt_time))

    def testWrapTrip(self):
        testTripJSON = {
            '_id': bo.ObjectId("55d8c47b7d65cb39ee983c2d"),
            'start_ts': 1436826360.200,
            'start_fmt_time': '2015-07-13 15:26:00.200000-07:00',
            'end_ts': 1436826360.493,
            'end_fmt_time': '2015-07-13 15:26:00.493000-07:00',
            'start_place': bo.ObjectId("55d8c47b7d65cb39ee983c2d"),
            'end_place': bo.ObjectId("55d8c47b7d65cb39ee983c2d"),
            'start_loc': {"coordinates": [-122, 37], "type": "Point"},
            'user_id': UUID('0763de67-f61e-3f5d-90e7-518e69793954')
        }
        trip = ecwt.Trip(testTripJSON)
        self.assertEqual(trip.get_id(), bo.ObjectId("55d8c47b7d65cb39ee983c2d"))
        self.assertEqual(trip.start_place, bo.ObjectId("55d8c47b7d65cb39ee983c2d"))
        self.assertEqual(trip.end_place, bo.ObjectId("55d8c47b7d65cb39ee983c2d"))
        self.assertTrue(isinstance(trip.start_loc, gj.Point))

    def testDedupList(self):
        import emission.core.wrapper.location as ecwl
        import emission.core.wrapper.transition as ecwt

        self.assertEqual(type(ecwe.Entry.get_dedup_list("background/filtered_location")),
                         list)
        self.assertIn("latitude", ecwe.Entry.get_dedup_list("background/filtered_location"))
        self.assertIn("ts", ecwe.Entry.get_dedup_list("background/filtered_location"))

        self.assertEqual(type(ecwe.Entry.get_dedup_list("statemachine/transition")),
                         list)
        self.assertIn("curr_state", ecwe.Entry.get_dedup_list("statemachine/transition"))
        self.assertIn("ts", ecwe.Entry.get_dedup_list("statemachine/transition"))

if __name__ == '__main__':
    etc.configLogging()
    unittest.main()
