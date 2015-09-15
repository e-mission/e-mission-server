# Test the class that supports usercache entries
# The main change here is that 

# Standard imports
import logging
import unittest
from uuid import UUID

# Our imports
import emission.core.wrapper.entry as ecwe
import emission.core.wrapper.motionactivity as ecwm

class TestEntry(unittest.TestCase):
    def testWrapLocation(self):
        testEntryJSON = {'_id': '55a4418c7d65cb39ee9737cf',
         'data': {'accuracy': 52.5,
          'altitude': 0,
          'bearing': 0,
          'elapsedRealtimeNanos': 100623898000000L,
          'latitude': 37.3885529,
          'longitude': -122.0879696,
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
        self.assertEquals(entry.metadata.key, 'background/location')
        self.assertEquals(entry.metadata.type, 'message')
        self.assertEquals(entry.data.latitude, 37.3885529)
        self.assertEquals(entry.data.longitude, -122.0879696)
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
        self.assertEquals(entry.metadata.key, 'background/motion_activity')
        self.assertEquals(entry.metadata.type, 'message')
        self.assertEquals(entry.data.type, ecwm.MotionTypes.TILTING)
        self.assertEquals(entry.data.confidence, 100)
        logging.debug("activity time = %s, written at %s (%s)" % 
            (entry.data.ts, entry.metadata.write_ts, entry.metadata.write_fmt_time))

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    unittest.main()
