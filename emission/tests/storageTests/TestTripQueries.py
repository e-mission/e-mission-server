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
import bson.json_util as bju
import numpy as np

# Our imports
import emission.storage.decorations.trip_queries as esdt
import emission.storage.decorations.analysis_timeseries_queries as esda

import emission.storage.timeseries.timequery as estt
import emission.storage.timeseries.abstract_timeseries as esta

import emission.net.api.usercache as enau

import emission.core.get_database as edb
import emission.core.wrapper.userlabel as ecul
import emission.core.wrapper.rawtrip as ecwrt
import emission.core.wrapper.section as ecwc
import emission.core.wrapper.stop as ecws
import emission.core.wrapper.entry as ecwe

import emission.tests.storageTests.analysis_ts_common as etsa
import emission.tests.common as etc

class TestTripQueries(unittest.TestCase):
    def setUp(self):
        self.testUserId = uuid.uuid3(uuid.NAMESPACE_URL, "mailto:test@test.me")
        edb.get_analysis_timeseries_db().delete_many({'user_id': self.testUserId})
    
    def tearDown(self):
        edb.get_analysis_timeseries_db().delete_many({'user_id': self.testUserId})
        edb.get_usercache_db().delete_many({'user_id': self.testUserId})

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

    def testUserInputForTripNoInputs(self):
        """
        Test the case in which the user has not provided any inputs
        """
        new_trip = self.create_fake_trip()
        user_input = esdt.get_user_input_for_trip(esda.RAW_TRIP_KEY, self.testUserId, new_trip.get_id(), "manual/mode_confirm")
        self.assertIsNone(user_input)

    def testUserInputForTripOneInputFromCache(self):
        """
        Test the case in which the user has provided exactly one input
        """
        MODE_CONFIRM_KEY = "manual/mode_confirm"

        new_trip = self.create_fake_trip()
        new_mc = ecul.Userlabel()
        new_mc["start_ts"] = new_trip.data.start_ts + 1
        new_mc["end_ts"] = new_trip.data.end_ts + 1
        new_mc["label"] = "roller_blading"
        new_mce = ecwe.Entry.create_entry(self.testUserId, MODE_CONFIRM_KEY, new_mc)
        new_mce["metadata"]["type"] = "message"

        enau.sync_phone_to_server(self.testUserId, [new_mce])
        
        user_input = esdt.get_user_input_from_cache_series(self.testUserId, new_trip, MODE_CONFIRM_KEY)

        self.assertEqual(new_mce, user_input)

    def testUserInputForTripOneInput(self):
        """
        Test the case in which the user has provided exactly one input
        """
        MODE_CONFIRM_KEY = "manual/mode_confirm"

        new_trip = self.create_fake_trip()
        new_mc = ecul.Userlabel()
        new_mc["start_ts"] = new_trip.data.start_ts + 1
        new_mc["end_ts"] = new_trip.data.end_ts + 1
        ts = esta.TimeSeries.get_time_series(self.testUserId)
        ts.insert_data(self.testUserId, MODE_CONFIRM_KEY, new_mc) 
        
        user_input = esdt.get_user_input_for_trip(esda.RAW_TRIP_KEY, self.testUserId,
            new_trip.get_id(), MODE_CONFIRM_KEY)

        self.assertEqual(new_mc, user_input.data)

    def testUserInputForTripTwoInputFromCache(self):
        """
        Test the case in which the user has provided exactly one input
        """
        MODE_CONFIRM_KEY = "manual/mode_confirm"

        new_trip = self.create_fake_trip()
        new_mc = ecul.Userlabel()
        new_mc["start_ts"] = new_trip.data.start_ts + 1
        new_mc["end_ts"] = new_trip.data.end_ts + 1
        new_mc["label"] = "roller_blading"
        new_mce = ecwe.Entry.create_entry(self.testUserId, MODE_CONFIRM_KEY, new_mc)
        new_mce["metadata"]["type"] = "message"

        enau.sync_phone_to_server(self.testUserId, [new_mce])

        user_input = esdt.get_user_input_from_cache_series(self.testUserId, new_trip, MODE_CONFIRM_KEY)

        # WHen there is only one input, it is roller_blading
        self.assertEqual(new_mce, user_input)
        self.assertEqual(ecwe.Entry(user_input).data.label, 'roller_blading')

        new_mc["label"] = 'pogo_sticking'

        new_mce = ecwe.Entry.create_entry(self.testUserId, MODE_CONFIRM_KEY, new_mc)
        new_mce["metadata"]["type"] = "message"

        enau.sync_phone_to_server(self.testUserId, [new_mce])

        user_input = esdt.get_user_input_from_cache_series(self.testUserId, new_trip, MODE_CONFIRM_KEY)

        # When it is overridden, it is pogo sticking
        self.assertEqual(new_mce, user_input)
        self.assertEqual(ecwe.Entry(user_input).data.label, 'pogo_sticking')


    def testUserInputForTripTwoInput(self):
        """
        Test the case in which the user has provided two inputs
        """
        MODE_CONFIRM_KEY = "manual/mode_confirm"

        ts = esta.TimeSeries.get_time_series(self.testUserId)

        new_trip = self.create_fake_trip()
        new_mc = ecul.Userlabel()
        new_mc["start_ts"] = new_trip.data.start_ts + 1
        new_mc["end_ts"] = new_trip.data.end_ts + 1
        new_mc["label"] = "car"
        ts.insert_data(self.testUserId, MODE_CONFIRM_KEY, new_mc) 
        user_input = esdt.get_user_input_for_trip(esda.RAW_TRIP_KEY, self.testUserId,
            new_trip.get_id(), MODE_CONFIRM_KEY)

        # WHen there is only one input, it is a car
        self.assertEqual(new_mc, user_input.data)
        self.assertEqual(user_input.data.label, "car")

        new_mc["label"] = "bike"
        ts.insert_data(self.testUserId, MODE_CONFIRM_KEY, new_mc) 
        
        user_input = esdt.get_user_input_for_trip(esda.RAW_TRIP_KEY, self.testUserId,
            new_trip.get_id(), MODE_CONFIRM_KEY)

        # When it is overridden, it is a bike
        self.assertEqual(new_mc, user_input.data)
        self.assertEqual(user_input.data.label, "bike")

    def testUserInputRealData(self):
        np.random.seed(61297777)
        dataFile = "emission/tests/data/real_examples/shankari_single_positional_indexer.dec-12"
        etc.setupRealExample(self, dataFile)
        self.testUserId = self.testUUID
        # At this point, we have only raw data, no trips
        etc.runIntakePipeline(self.testUUID)
        # At this point, we have trips

        # Let's retrieve them
        ts = esta.TimeSeries.get_time_series(self.testUUID)
        ct_df = ts.get_data_df("analysis/cleaned_trip", time_query=None)
        self.assertEqual(len(ct_df), 4)

        # Now, let's load the mode_confirm and purpose_confirm objects
        mode_confirm_list = json.load(open("emission/tests/data/real_examples/shankari_single_positional_indexer.dec-12.mode_confirm"),
            object_hook=bju.object_hook)
        self.assertEqual(len(mode_confirm_list), 5)

        purpose_confirm_list = json.load(open("emission/tests/data/real_examples/shankari_single_positional_indexer.dec-12.purpose_confirm"),
            object_hook=bju.object_hook)
        self.assertEqual(len(purpose_confirm_list), 7)

        for mc in mode_confirm_list:
            mc["user_id"] = self.testUUID
            ts.insert(mc)

        for pc in purpose_confirm_list:
            pc["user_id"] = self.testUUID
            ts.insert(pc)

        mc_label_list = []
        pc_label_list = []
        for trip_id in ct_df._id:
            mc = esdt.get_user_input_for_trip(esda.CLEANED_TRIP_KEY,
                        self.testUserId, ct_df._id[0], "manual/mode_confirm")
            mc_label_list.append(mc.data.label)

            pc = esdt.get_user_input_for_trip(esda.CLEANED_TRIP_KEY,
                        self.testUserId, ct_df._id[0], "manual/purpose_confirm")
            pc_label_list.append(pc.data.label)

        self.assertEqual(mc_label_list, 4 * ['bike'])
        self.assertEqual(pc_label_list, 4 * ['pick_drop'])

if __name__ == '__main__':
    import emission.tests.common as etc
    etc.configLogging()
    unittest.main()
