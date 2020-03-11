from __future__ import unicode_literals
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import
# Standard imports
from future import standard_library
standard_library.install_aliases()
from builtins import *
import unittest
import datetime as pydt
import logging
import json
from uuid import UUID
import attrdict as ad
import geojson

import emission.tests.common as etc

# Our imports
import emission.net.usercache.formatters.formatter as enuf
import emission.core.wrapper.motionactivity as ema
import emission.core.wrapper.transition as et

# These are the current formatters, so they are included here for testing.
# However, it is unclear whether or not we need to add other tests as we add other formatters,
# specially if they follow the same pattern.

class TestFormatters(unittest.TestCase):
    def testConvertMotionActivity(self):
        with open("emission/tests/data/netTests/android.activity.txt") as fp:
            entry = json.load(fp)
        logging.debug("entry.keys() = %s" % list(entry.keys()))
        formatted_entry = enuf.convert_to_common_format(ad.AttrDict(entry))
        self.assertEquals(formatted_entry.data.confidence, 100)
        self.assertEquals(formatted_entry.data.type, ema.MotionTypes.TILTING.value)
        self.assertEquals(formatted_entry.data.ts, 1436826360.493)
        self.assertTrue(formatted_entry.data.fmt_time.startswith("2015-07-13T15:26:00.493"))

    def testConvertLocation(self):
        with open("emission/tests/data/netTests/android.location.raw.txt") as fp:
            entry = json.load(fp)
        formatted_entry = enuf.convert_to_common_format(ad.AttrDict(entry))
        self.assertEquals(formatted_entry.data.accuracy, 52.5)
        self.assertEquals(formatted_entry.data.latitude, 37.3885529)
        self.assertEquals(formatted_entry.data.longitude, -122.0879696)
        self.assertEquals(formatted_entry.data.loc, geojson.Point((-122.0879696, 37.3885529)))
        self.assertEquals(formatted_entry.data.ts, 1436826356.852)
        self.assertTrue(formatted_entry.data.fmt_time.startswith("2015-07-13T15:25:56.852"))
        self.assertEquals(formatted_entry.metadata.write_ts, 1436826357.115)
        self.assertTrue(formatted_entry.metadata.write_fmt_time.startswith("2015-07-13T15:25:57.115"))

    def testConvertTransition(self):
        with open("emission/tests/data/netTests/android.transition.txt") as fp:
            entry = json.load(fp)
        formatted_entry = enuf.convert_to_common_format(ad.AttrDict(entry))
        self.assertEquals(formatted_entry.data.curr_state, et.State.WAITING_FOR_TRIP_START.value)
        self.assertEquals(formatted_entry.data.transition, et.TransitionType.INITIALIZE.value)
        self.assertEquals(formatted_entry.metadata.write_ts, 1436821510.445)
        self.assertTrue(formatted_entry.data.fmt_time.startswith("2015-07-13T14:05:10.445"))

    def testFlagsToEnumOneEntry(self):
        import emission.net.usercache.formatters.ios.motion_activity as ioma
        with open("emission/tests/data/netTests/ios.activity.txt") as fp:
            entry = json.load(fp)
        data = entry["data"]
        enum = ioma.type_flags_to_enum(data)
        self.assertEqual(enum, ema.MotionTypes.STILL)
        
    def testFlagsToEnumStoppedInVehicle(self):
        import emission.net.usercache.formatters.ios.motion_activity as ioma
        with open("emission/tests/data/netTests/ios.activity.txt") as fp:
            entry = json.load(fp)
        data = entry["data"]
        data["automotive"] = True
        enum = ioma.type_flags_to_enum(data)
        self.assertEqual(enum, ema.MotionTypes.STOPPED_WHILE_IN_VEHICLE)
        
    def testFlagsToEnumTwoEntries(self):
        import emission.net.usercache.formatters.ios.motion_activity as ioma
        with open("emission/tests/data/netTests/ios.activity.txt") as fp:
            entry = json.load(fp)
        data = entry["data"]
        data["cycling"] = True
        with self.assertRaisesRegexp(RuntimeError, ".*two modes.*"):
            enum = ioma.type_flags_to_enum(data)
            logging.warning("Got result num = %s instead of raising exception" % enum)
    
    def testFlagsToEnumNoEntries(self):
        import emission.net.usercache.formatters.ios.motion_activity as ioma
        with open("emission/tests/data/netTests/ios.activity.none.txt") as fp:
            entry = json.load(fp)
        data = entry["data"]
        enum = ioma.type_flags_to_enum(data)
        self.assertEqual(enum, ema.MotionTypes.NONE)
            
    def testConvertMotionActivity_ios(self):
        with open("emission/tests/data/netTests/ios.activity.txt") as fp:
            entry = json.load(fp)
        formatted_entry = enuf.convert_to_common_format(ad.AttrDict(entry))
        self.assertEquals(formatted_entry.data.confidence, 100)
        self.assertEquals(formatted_entry.data.type, ema.MotionTypes.STILL.value)
        self.assertEquals(formatted_entry.data.ts, 1446513827.479381)
        self.assertTrue(formatted_entry.data.fmt_time.startswith("2015-11-02T17:23:47"))
        
    def testConvertLocation_ios(self):
        with open("emission/tests/data/netTests/ios.location.txt") as fp:
            entry = json.load(fp)
        formatted_entry = enuf.convert_to_common_format(ad.AttrDict(entry))
        self.assertEquals(formatted_entry.data.accuracy, 65)
        self.assertEquals(formatted_entry.data.latitude, 37.39974810579324)
        self.assertEquals(formatted_entry.data.longitude, -122.0808742899394)
        self.assertEquals(formatted_entry.data.loc, geojson.Point((-122.0808742899394, 37.39974810579324)))
        self.assertEquals(formatted_entry.data.ts, 1446503965.190834)
        self.assertTrue(formatted_entry.data.fmt_time.startswith("2015-11-02T14:39:25.190"))
        self.assertEquals(formatted_entry.metadata.write_ts, 1446503965.760821)
        self.assertTrue(formatted_entry.metadata.write_fmt_time.startswith("2015-11-02T14:39:25.760"))
        
    def testConvertTransition_ios(self):
        with open("emission/tests/data/netTests/ios.transition.txt") as fp:
            entry = json.load(fp)
        formatted_entry = enuf.convert_to_common_format(ad.AttrDict(entry))
        self.assertEquals(formatted_entry.data.curr_state, et.State.WAITING_FOR_TRIP_START.value)
        self.assertEquals(formatted_entry.data.transition, et.TransitionType.STOPPED_MOVING.value)
        self.assertEquals(formatted_entry.metadata.write_ts, 1446577206.122407)
        self.assertTrue(formatted_entry.data.fmt_time.startswith("2015-11-03T11:00:06.122"))


if __name__ == '__main__':
    import emission.tests.common as etc

    etc.configLogging()
    unittest.main()
