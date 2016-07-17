# Standard imports
import unittest
import datetime as pydt
import logging
import uuid
import json

# Our imports
import emission.storage.decorations.trip_queries as esdt
import emission.storage.decorations.analysis_timeseries_queries as esda

import emission.storage.timeseries.timequery as estt
import emission.storage.timeseries.abstract_timeseries as esta

import emission.core.get_database as edb
import emission.core.wrapper.rawtrip as ecwrt
import emission.core.wrapper.section as ecwc
import emission.core.wrapper.stop as ecws

import emission.tests.storageTests.analysis_ts_common as etsa

class TestTripQueries(unittest.TestCase):
    def setUp(self):
        self.testUserId = uuid.uuid3(uuid.NAMESPACE_URL, "mailto:test@test.me")
        edb.get_analysis_timeseries_db().remove({'user_id': self.testUserId})

    def create_fake_trip(self):
        return etsa.createNewTripLike(self, esda.RAW_TRIP_KEY, ecwrt.Rawtrip)

    def testGetTimeRangeForTrip(self):
        new_trip = self.create_fake_trip()
        ret_tq = esda.get_time_query_for_trip_like(esda.RAW_TRIP_KEY, new_trip.get_id())
        self.assertEqual(ret_tq.timeType, "data.ts")
        self.assertEqual(ret_tq.startTs, 5)
        self.assertEqual(ret_tq.endTs, 6)

    def testQuerySectionsForTrip(self):
        new_trip = self.create_fake_trip()
        new_section = ecwc.Section()
        new_section.trip_id = new_trip.get_id()
        new_section.start_ts = 5
        new_section.end_ts = 6
        ts = esta.TimeSeries.get_time_series(self.testUserId)
        ts.insert_data(self.testUserId, esda.RAW_SECTION_KEY, new_section) 
        ret_entries = esdt.get_raw_sections_for_trip(self.testUserId, new_trip.get_id())
        self.assertEqual([entry.data for entry in ret_entries], [new_section])

    def testQueryStopsForTrip(self):
        new_trip = self.create_fake_trip()
        new_stop = ecws.Stop()
        new_stop.trip_id = new_trip.get_id()
        new_stop.enter_ts = 5
        new_stop.exit_ts = 6
        ts = esta.TimeSeries.get_time_series(self.testUserId)
        ts.insert_data(self.testUserId, esda.RAW_STOP_KEY, new_stop) 
        ret_entries = esdt.get_raw_stops_for_trip(self.testUserId, new_trip.get_id())
        self.assertEqual([entry.data for entry in ret_entries], [new_stop])

if __name__ == '__main__':
    import emission.tests.common as etc
    etc.configLogging()
    unittest.main()
