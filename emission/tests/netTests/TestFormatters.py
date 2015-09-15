# Standard imports
import unittest
import datetime as pydt
import logging
import json
from uuid import UUID
import attrdict as ad

# Our imports
import emission.net.usercache.formatters.formatter as enuf
import emission.core.wrapper.motionactivity as ema
import emission.core.wrapper.transition as et

# These are the current formatters, so they are included here for testing.
# However, it is unclear whether or not we need to add other tests as we add other formatters,
# specially if they follow the same pattern.

class TestFormatters(unittest.TestCase):
    def testConvertMotionActivity(self):
        entry = json.load(open("emission/tests/data/netTests/android.activity.txt"))
        logging.debug("entry.keys() = %s" % entry.keys())
        formatted_entry = enuf.convert_to_common_format(ad.AttrDict(entry))
        self.assertEquals(formatted_entry.data.confidence, 100)
        self.assertEquals(formatted_entry.data.type, ema.MotionTypes.TILTING.value)
        self.assertEquals(formatted_entry.data.ts, 1436826360.493)
        self.assertTrue(formatted_entry.data.fmt_time.startswith("2015-07-13 15:26:00.493"))

    def testConvertLocation(self):
        entry = json.load(open("emission/tests/data/netTests/android.location.raw.txt"))
        formatted_entry = enuf.convert_to_common_format(ad.AttrDict(entry))
        self.assertEquals(formatted_entry.data.accuracy, 52.5)
        self.assertEquals(formatted_entry.data.latitude, 37.3885529)
        self.assertEquals(formatted_entry.data.longitude, -122.0879696)
        self.assertEquals(formatted_entry.data.ts, 1436826356.852)
        self.assertTrue(formatted_entry.data.fmt_time.startswith("2015-07-13 15:25:56.852"))
        self.assertEquals(formatted_entry.metadata.write_ts, 1436826357.115)
        self.assertTrue(formatted_entry.metadata.write_fmt_time.startswith("2015-07-13 15:25:57.115"))

    def testConvertTransition(self):
        entry = json.load(open("emission/tests/data/netTests/android.transition.txt"))
        formatted_entry = enuf.convert_to_common_format(ad.AttrDict(entry))
        self.assertEquals(formatted_entry.data.curr_state, et.State.WAITING_FOR_TRIP_START.value)
        self.assertEquals(formatted_entry.data.transition, et.TransitionType.INITIALIZE.value)
        self.assertEquals(formatted_entry.metadata.write_ts, 1436821510.445)
        self.assertTrue(formatted_entry.data.fmt_time.startswith("2015-07-13 14:05:10.445"))

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    unittest.main()
