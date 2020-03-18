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
import uuid
import json

# Our imports
import emission.storage.decorations.stop_queries as esds
import emission.storage.decorations.analysis_timeseries_queries as esda
import emission.storage.timeseries.abstract_timeseries as esta
import emission.storage.timeseries.builtin_timeseries as estb
import emission.storage.timeseries.timequery as estt

import emission.core.get_database as edb
import emission.core.wrapper.stop as ecws

# Our test imports
import emission.tests.storageTests.analysis_ts_common as etsa

class TestStopQueries(unittest.TestCase):
    def setUp(self):
        self.testUserId = uuid.uuid3(uuid.NAMESPACE_URL, "mailto:test@test.me")
        edb.get_analysis_timeseries_db().delete_many({'user_id': self.testUserId})
        self.test_trip_id = "test_trip_id"

    def testQueryStops(self):
        new_stop = etsa.savePlaceLike(self, esda.RAW_STOP_KEY, ecws.Stop)
        new_stop["data"]["trip_id"] = self.test_trip_id
        estb.BuiltinTimeSeries.update(new_stop)
        ret_arr_one = esds.get_stops_for_trip(self.testUserId, self.test_trip_id)
        self.assertEqual(len(ret_arr_one), 1)
        self.assertEqual(ret_arr_one, [new_stop])
        ret_arr_list = esds.get_stops_for_trip_list(self.testUserId, [self.test_trip_id])
        self.assertEqual(ret_arr_one, ret_arr_list)
        ret_arr_time = esda.get_objects(esda.RAW_STOP_KEY, self.testUserId,
            estt.TimeQuery("data.enter_ts", 4, 6))
        self.assertEqual([entry.data for entry in ret_arr_list], ret_arr_time)

if __name__ == '__main__':
    import emission.tests.common as etc
    etc.configLogging()
    unittest.main()
