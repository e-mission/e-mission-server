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
import copy

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
        edb.get_timeseries_db().delete_many({'user_id': self.testUserId})
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
        new_mc["label"] = "pogo_sticking"
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
        self.assertEqual(user_input.data.label, 'roller_blading')

        new_mc["label"] = 'pogo_sticking'

        new_mce = ecwe.Entry.create_entry(self.testUserId, MODE_CONFIRM_KEY, new_mc)
        new_mce["metadata"]["type"] = "message"

        enau.sync_phone_to_server(self.testUserId, [new_mce])

        user_input = esdt.get_user_input_from_cache_series(self.testUserId, new_trip, MODE_CONFIRM_KEY)

        # When it is overridden, it is pogo sticking
        self.assertEqual(new_mce, user_input)
        self.assertEqual(user_input.data.label, 'pogo_sticking')


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
        with open("emission/tests/data/real_examples/shankari_single_positional_indexer.dec-12.mode_confirm") as mcfp:
            mode_confirm_list = json.load(mcfp, object_hook=bju.object_hook)
        self.assertEqual(len(mode_confirm_list), 5)

        with open("emission/tests/data/real_examples/shankari_single_positional_indexer.dec-12.purpose_confirm") as pcfp:
            purpose_confirm_list = json.load(pcfp, object_hook=bju.object_hook)
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
                        self.testUserId, trip_id, "manual/mode_confirm")
            mc_label_list.append(mc.data.label)

            pc = esdt.get_user_input_for_trip(esda.CLEANED_TRIP_KEY,
                        self.testUserId, trip_id, "manual/purpose_confirm")
            pc_label_list.append(pc.data.label)

        self.assertEqual(mc_label_list, 4 * ['bike'])
        self.assertEqual(pc_label_list, 4 * ['pick_drop'])

    def testUserInputRealDataPostArrival(self):
        np.random.seed(61297777)
        dataFile = "emission/tests/data/real_examples/shankari_single_positional_indexer.dec-12"
        etc.setupRealExample(self, dataFile)
        self.testUserId = self.testUUID
        # At this point, we have only raw data, no trips
        etc.runIntakePipeline(self.testUUID)
        # At this point, we have trips

        # Let's retrieve them
        ts = esta.TimeSeries.get_time_series(self.testUUID)
        ct_df = ts.get_data_df("analysis/confirmed_trip", time_query=None)
        self.assertEqual(len(ct_df), 4)
        mode_fmt_times = list(ct_df.start_fmt_time)
        # corresponds to the walk not a trip
        # https://github.com/e-mission/e-mission-docs/issues/476#issuecomment-747115640)
        mode_fmt_times.insert(3, None)
        purpose_fmt_times = copy.copy(mode_fmt_times)
        # corresponds to overrides for the same trip
        # they are correctly matched to the same trip
        # in the final pipeline step, will override the same entry multiple times
        purpose_fmt_times.insert(3, purpose_fmt_times[1])
        purpose_fmt_times.insert(4, purpose_fmt_times[0])
        print("expected_fmt_times: mode = %s" % mode_fmt_times)
        print("expected_fmt_times: purpose = %s" % purpose_fmt_times)

        # Now, let's load the mode_confirm and purpose_confirm objects
        with open("emission/tests/data/real_examples/shankari_single_positional_indexer.dec-12.mode_confirm") as mcfp:
            mode_confirm_list = [ecwe.Entry(mc) for mc in json.load(mcfp, object_hook=bju.object_hook)]
        self.assertEqual(len(mode_confirm_list), 5)

        with open("emission/tests/data/real_examples/shankari_single_positional_indexer.dec-12.purpose_confirm") as pcfp:
            purpose_confirm_list = [ecwe.Entry(pc) for pc in json.load(pcfp, object_hook=bju.object_hook)]
        self.assertEqual(len(purpose_confirm_list), 7)

        mc_trip_start_fmt_time_list = []
        pc_trip_start_fmt_time_list = []
        for mode in mode_confirm_list:
            mc_trip = esdt.get_trip_for_user_input_obj(ts, mode)
            mc_trip_start_fmt_time_list.append(mc_trip.data.start_fmt_time if mc_trip is not None else None)

        for purpose in purpose_confirm_list:
            pc_trip = esdt.get_trip_for_user_input_obj(ts, purpose)
            print("Found pc_trip %s" % pc_trip.data.start_fmt_time if pc_trip is not None else None)
            pc_trip_start_fmt_time_list.append(pc_trip.data.start_fmt_time if pc_trip is not None else None)

        self.assertEqual(mc_trip_start_fmt_time_list, mode_fmt_times)
        self.assertEqual(pc_trip_start_fmt_time_list, purpose_fmt_times)

    def testFilterLabelInputs(self):
        import pandas as pd

        # Test invalid inputs
        pd.testing.assert_frame_equal(esdt.filter_labeled_trips(pd.DataFrame()),
            pd.DataFrame())
        with self.assertRaises(TypeError):
             esdt.filter_labeled_trips(None)

        # Test valid inputs
        
        # no labeled
        test_unlabeled_df = pd.DataFrame([{"user_input": {}}] * 3)
        self.assertTrue(esdt.filter_labeled_trips(test_unlabeled_df).empty)

        # all labeled
        test_labeled_df = pd.DataFrame([{"user_input": {"mode_confirm": "bike", "purpose_confirm": "shopping"}}] * 3)
        pd.testing.assert_frame_equal(esdt.filter_labeled_trips(test_labeled_df),
            test_labeled_df)

        # mixed labeled
        test_mixed_df = pd.DataFrame(([{"user_input": {"mode_confirm": "bike", "purpose_confirm": "shopping"}}] * 3 + [{"user_input": {}}] * 3))
        result_df = pd.DataFrame([{"user_input": {"mode_confirm": "bike", "purpose_confirm": "shopping"}}] * 3)
        pd.testing.assert_frame_equal(esdt.filter_labeled_trips(test_mixed_df),
            result_df)

    def testExpandUserInputs(self):
        import pandas as pd
        import numpy as np

        # Test invalid inputs
        pd.testing.assert_frame_equal(esdt.expand_userinputs(pd.DataFrame()),
            pd.DataFrame())
        with self.assertRaises(TypeError):
             esdt.expand_userinputs(None)

        # Test valid inputs
        
        # no labeled trips; no additional columns added
        logging.debug("About to test unlabeled")
        test_unlabeled_df = pd.DataFrame([{"user_input": {}}] * 3)
        pd.testing.assert_frame_equal(esdt.expand_userinputs(test_unlabeled_df),
            test_unlabeled_df)

        # all labeled; additional columns added with unstructured data
        test_labeled_df = pd.DataFrame([{"user_input": {"mode_confirm": "bike", "purpose_confirm": "shopping"}}] * 3)
        test_exp_result = pd.DataFrame([{"user_input": {"mode_confirm": "bike", "purpose_confirm": "shopping"}, "mode_confirm": "bike", "purpose_confirm" : "shopping"}] * 3)
        logging.debug(test_exp_result)
        pd.testing.assert_frame_equal(esdt.expand_userinputs(test_labeled_df),
            test_exp_result)

        # mixed labeled; additional columns added but with some N/A
        test_mixed_df = pd.DataFrame(([{"user_input": {"mode_confirm": "bike", "purpose_confirm": "shopping"}}] * 3 + [{"user_input": {}}] * 3))
        result_df = pd.DataFrame(
        # Three expanded entries
            [{"user_input": {"mode_confirm": "bike", "purpose_confirm": "shopping"}, "mode_confirm": "bike", "purpose_confirm" : "shopping"}] * 3 + 
        # Three partial entries
            [{"user_input": {}}] * 3)
        actual_result = esdt.expand_userinputs(test_mixed_df)
        pd.testing.assert_frame_equal(actual_result, result_df)
        # The last three entries are N/A
        self.assertTrue(pd.isna(actual_result.loc[3, "mode_confirm"]))

        # mixed labeled with different columns; additional columns added but with some N/A
        test_mixed_df = pd.DataFrame((
            [{"user_input": {"mode_confirm": "bike", "purpose_confirm": "shopping"}}] * 3 +
            [{"user_input": {"mode_confirm": "bike", "purpose_confirm": "shopping", "replaced_mode": "running"}}] * 3 +
            [{"user_input": {}}] * 3))
        result_df = pd.DataFrame(
        # Three partially expanded entries
            [{"user_input": {"mode_confirm": "bike", "purpose_confirm": "shopping"}, "mode_confirm": "bike", "purpose_confirm" : "shopping"}] * 3 + 
        # Three fully expanded entries
            [{"user_input": {"mode_confirm": "bike", "purpose_confirm": "shopping", "replaced_mode": "running"}, "mode_confirm": "bike", "purpose_confirm" : "shopping", "replaced_mode": "running"}] * 3 + 
        # Three partial entries
            [{"user_input": {}}] * 3)
        actual_result = esdt.expand_userinputs(test_mixed_df)
        pd.testing.assert_frame_equal(actual_result, result_df)
        # The first three entries have N/A replaced mode
        logging.debug(pd.isna(actual_result.loc[:2, "replaced_mode"]))
        self.assertTrue(pd.isna(actual_result.loc[:2, "replaced_mode"]).all())
        # The last three entries have N/A for all expanded values
        logging.debug(pd.isna(actual_result.loc[6:,["mode_confirm", "purpose_confirm", "replaced_mode"]]))
        self.assertTrue(pd.isna(actual_result.loc[6:,["mode_confirm", "purpose_confirm", "replaced_mode"]].to_numpy().flatten()).all())

if __name__ == '__main__':
    import emission.tests.common as etc
    etc.configLogging()
    unittest.main()
