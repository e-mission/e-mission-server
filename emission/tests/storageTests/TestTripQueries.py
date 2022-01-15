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
import pandas as pd
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
import emission.core.wrapper.confirmedtrip as ecwct

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

    def testHasFinalLabels(self):
        import pandas as pd

        # no user input, no high-confidence inference (yellow "To Label" case)
        self.assertFalse(esdt.has_final_labels(ecwct.Confirmedtrip({
            "user_input": {},
            "expectation": {"to_label": True}
        })))

        # no user input, but we do have a high-confidence inference (yellow
        # "All unlabeled" case)
        self.assertTrue(esdt.has_final_labels(ecwct.Confirmedtrip({
            "user_input": {},
            "expectation": {"to_label": False}
        })))

        # user input without a high-confidence inference (red -> green label case)
        self.assertTrue(esdt.has_final_labels(ecwct.Confirmedtrip({
            "user_input": {"mode_confirm": "drove_alone", "purpose_confirm": "work"},
            "expectation": {"to_label": True}
        })))

        # user input with a high-confidence inference (yellow -> green label case)
        self.assertTrue(esdt.has_final_labels(ecwct.Confirmedtrip({
            "user_input": {"mode_confirm": "drove_alone", "purpose_confirm": "work"},
            "expectation": {"to_label": False}
        })))

    def testGetMaxProbLabel(self):
        self.assertEqual(esdt.get_max_prob_label([
            {'labels': {'mc': 30, 'pc': 40}, 'p': 0.9}]), {'mc': 30, 'pc': 40})
        self.assertEqual(esdt.get_max_prob_label([
            {'labels': {'mc': 30, 'pc': 40}, 'p': 0.08249999999999999},
            {'labels': {'mc': 50, 'pc': 60}, 'p': 0.9075}]),
            {'mc': 50, 'pc': 60})
        self.assertEqual(esdt.get_max_prob_label([
            {'labels': {'mc': 10, 'pc': 20}, 'p': 0.043040248408698994},
            {'labels': {'mc': 31, 'pc': 41}, 'p': 0.9038452165826789},
            {'labels': {'mc': 50, 'pc': 60 }, 'p': 0.043040248408698994}]),
            {'mc': 31, 'pc': 41})

    def testExpandFinalLabelsPandasFunctions(self):
        import pandas as pd
        all_df = pd.DataFrame([
            {"trip": "t1", "mc": 10, "pc": 20},
            {"trip": "t2", "mc": 30, "pc": 40},
            {"trip": "t3", "mc": 10, "pc": 20},
            {"trip": "t4", "mc": 30, "pc": 40},
            {"trip": "t5", "mc": 10, "pc": 20},
            {"trip": "t6", "mc": 30, "pc": 40},
            {"trip": "t7", "mc": 10, "pc": 20}])

        labeled_df = all_df[all_df.mc == 10]
        inferred_df = all_df[all_df.mc == 30]

        logging.debug("Testing naive vs. reindexed concat")
        # Naive concat will not concatenate values correctly because the
        # indices will be out of order
        naive_concat = pd.concat([labeled_df, inferred_df], axis=0)
        logging.debug(naive_concat)
        try:
            pd.testing.assert_index_equal(naive_concat.index, all_df.index)
        except AssertionError as e:
            logging.info(e)

        # We need to reindex so that the values are back in order
        correct_concat = naive_concat.reindex(all_df.index)
        logging.debug(correct_concat)
        pd.testing.assert_index_equal(correct_concat.index, all_df.index)

        logging.debug("Testing concat with one dataframe empty")
        labeled_df = all_df[all_df.mc == 100]
        inferred_df = all_df[all_df.mc == 30]
        naive_concat = pd.concat([labeled_df, inferred_df], axis=0)
        # The naive re-index fails with the error
        # ValueError: cannot reindex from a duplicate axis
        # since we have N/A entries for the labeled df entries
        # We need to drop the N/A first as seen below
        try:
            correct_concat = naive_concat.reindex(all_df.index)
        except ValueError as e:
            logging.info(e)

        logging.debug("Testing concat with one dataframe empty and forced index")
        labeled_df = all_df[all_df.mc == 100].reindex(all_df.index)
        inferred_df = all_df[all_df.mc == 30]
        labeled_df.dropna('index', how="all", inplace=True)
        naive_concat = pd.concat([labeled_df, inferred_df], axis=0)
        logging.debug(naive_concat)
        correct_concat = naive_concat.reindex(all_df.index)
        logging.debug(correct_concat)

    def testExpandFinalLabelsPandasFunctionsNestedPostFilter(self):
        import pandas as pd
        nested_df = pd.DataFrame([
            {"trip": "t1", "user_input": {},
                "expectation" : {"to_label": True},
                "inferred_labels": [{"labels": {"mc": 10, "pc": 20}, "p": 0.2}]},
            {"trip": "t2", "user_input": {},
                "expectation" : {"to_label": False},
                "inferred_labels": [{"labels": {"mc": 30, "pc": 40}, "p": 0.9}]},
            {"trip": "t3", "user_input": {},
                "expectation" : {"to_label": False},
                "inferred_labels": [{"labels": {"mc": 50, "pc": 60}, "p": 0.9},
                                    {"labels": {"mc": 70, "pc": 80}, "p": 0.1}]},
            {"trip": "t4", "user_input": {"mc": 100, "pc": 200},
                "expectation" : {"to_label": False},
                "inferred_labels": [{"labels": {"mc": 90, "pc": 91}, "p": 0.1},
                                    {"labels": {"mc": 92, "pc": 93}, "p": 0.9}]}
        ])

        # logging.debug(nested_df.expectation)
        # we cannot use a nested query like this. Instead, let's expand the
        # expectation and then do some merging
        self.assertEqual(len(nested_df[nested_df.expectation == {"to_label" == False}]), 0)
        expectation_expansion = pd.DataFrame(nested_df.expectation.to_list(), index=nested_df.index)
        # logging.debug(expectation_expansion)
        # logging.debug(nested_df[expectation_expansion.to_label == False])
        self.assertEqual(nested_df[expectation_expansion.to_label == False].index.to_list(), [1, 2, 3])
        self.assertEqual(nested_df[expectation_expansion.to_label == False].trip.to_list(), ["t2", "t3", "t4"])
        logging.debug(nested_df[expectation_expansion.to_label == False].inferred_labels)
        high_confidence_inferred_labels = nested_df[expectation_expansion.to_label == False].inferred_labels
        # logging.debug(pd.DataFrame(high_confidence_inferred_labels))
        high_confidence_max_p_inferred_labels = high_confidence_inferred_labels.apply(esdt.get_max_prob_label)
        logging.debug(high_confidence_max_p_inferred_labels)
        pd.testing.assert_series_equal(high_confidence_max_p_inferred_labels,
            pd.Series([{'mc': 30, 'pc': 40}, {'mc': 50, 'pc': 60}, {'mc': 92, 'pc': 93}],
                index=range(1,4),
                name="inferred_labels"))
        high_confidence_max_p_inferred_labels_only = \
            pd.DataFrame(high_confidence_max_p_inferred_labels.to_list(),
            index=high_confidence_inferred_labels.index)
        logging.debug(high_confidence_max_p_inferred_labels_only)

        self.assertEqual(high_confidence_max_p_inferred_labels_only.mc.to_list(),
            [30, 50, 92])
        self.assertEqual(high_confidence_max_p_inferred_labels_only.pc.to_list(),
            [40, 60, 93])
        self.assertEqual(high_confidence_max_p_inferred_labels_only.index.to_list(),
            [1, 2, 3])

        # but t4 already has a user input, so we need to filter it out
        # Note that we could have entries that have both user inputs and high
        # confidence inferred values. This could happen if the user chooses to go
        # into "All Labels" and label high-confidence values.
        high_confidence_max_p_inferred_labels_only = \
            high_confidence_max_p_inferred_labels_only[nested_df.user_input == {}]
        logging.debug(high_confidence_max_p_inferred_labels_only)

        self.assertEqual(high_confidence_max_p_inferred_labels_only.mc.to_list(),
            [30, 50])
        self.assertEqual(high_confidence_max_p_inferred_labels_only.pc.to_list(),
            [40, 60])
        self.assertEqual(high_confidence_max_p_inferred_labels_only.index.to_list(),
            [1, 2])

    def testExpandFinalLabelsPandasFunctionsNestedPreFilter(self):
        import pandas as pd
        nested_df = pd.DataFrame([
            {"trip": "t1", "user_input": {},
                "expectation" : {"to_label": True},
                "inferred_labels": [{"labels": {"mc": 10, "pc": 20}, "p": 0.2}]},
            {"trip": "t2", "user_input": {},
                "expectation" : {"to_label": False},
                "inferred_labels": [{"labels": {"mc": 30, "pc": 40}, "p": 0.9}]},
            {"trip": "t3", "user_input": {},
                "expectation" : {"to_label": False},
                "inferred_labels": [{"labels": {"mc": 50, "pc": 60}, "p": 0.9},
                                    {"labels": {"mc": 70, "pc": 80}, "p": 0.1}]},
            {"trip": "t4", "user_input": {"mc": 100, "pc": 200},
                "expectation" : {"to_label": False},
                "inferred_labels": [{"labels": {"mc": 90, "pc": 91}, "p": 0.1},
                                    {"labels": {"mc": 92, "pc": 93}, "p": 0.9}]}
        ])

        # logging.debug(nested_df.expectation)
        # we cannot use a nested query like this. Instead, let's expand the
        # expectation and then do some merging
        self.assertEqual(len(nested_df[nested_df.expectation == {"to_label" == False}]), 0)
        expectation_expansion = pd.DataFrame(nested_df.expectation.to_list(), index=nested_df.index)
        # logging.debug(expectation_expansion)
        # logging.debug(nested_df[expectation_expansion.to_label == False])
        # logging.debug(nested_df.user_input == {})
        # logging.debug(expectation_expansion.to_label == False)

        # t4 already has a user input, so we need to filter it out
        # here, we filter first before the expansion
        self.assertEqual(nested_df[(nested_df.user_input == {}) & (expectation_expansion.to_label == False)].index.to_list(), [1, 2])
        self.assertEqual(nested_df[(nested_df.user_input == {}) & (expectation_expansion.to_label == False)].trip.to_list(), ["t2", "t3"])
        logging.debug(nested_df[(nested_df.user_input == {}) & (expectation_expansion.to_label == False)].inferred_labels)
        high_confidence_inferred_labels = nested_df[(nested_df.user_input == {}) & (expectation_expansion.to_label == False)].inferred_labels
        # logging.debug(pd.DataFrame(high_confidence_inferred_labels))
        high_confidence_max_p_inferred_labels = high_confidence_inferred_labels.apply(esdt.get_max_prob_label)
        logging.debug(high_confidence_max_p_inferred_labels)
        pd.testing.assert_series_equal(high_confidence_max_p_inferred_labels,
            pd.Series([{'mc': 30, 'pc': 40}, {'mc': 50, 'pc': 60}],
                index=range(1,3),
                name="inferred_labels"))
        high_confidence_max_p_inferred_labels_only = \
            pd.DataFrame(high_confidence_max_p_inferred_labels.to_list(),
            index=high_confidence_inferred_labels.index)
        logging.debug(high_confidence_max_p_inferred_labels_only)

        self.assertEqual(high_confidence_max_p_inferred_labels_only.mc.to_list(),
            [30, 50])
        self.assertEqual(high_confidence_max_p_inferred_labels_only.pc.to_list(),
            [40, 60])
        self.assertEqual(high_confidence_max_p_inferred_labels_only.index.to_list(),
            [1, 2])

        # Check that none of the high confidence expanded labels has a user input
        self.assertTrue(pd.Series(nested_df.loc[
            high_confidence_max_p_inferred_labels_only.index].user_input == {}).all())

    def testExpandFinalLabels(self):

        # Test invalid inputs
        pd.testing.assert_frame_equal(esdt.expand_finallabels(pd.DataFrame()),
            pd.DataFrame())
        with self.assertRaises(TypeError):
             esdt.expand_finallabels(None)

        # Test valid inputs

        # no labeled trips

        # all red trips; no additional columns added
        logging.debug("About to test unlabeled + uninferred")
        test_unlabeled_df = pd.DataFrame([{"user_input": {},
            "expectation": {"to_label": True}}] * 3)
        pd.testing.assert_frame_equal(esdt.expand_finallabels(test_unlabeled_df),
            test_unlabeled_df)

        # all high-confidence yellow; mode_confirm and purpose_confirm columns added
        logging.debug("About to test unlabeled + all inferred")
        test_unlabeled_df = pd.DataFrame([{"user_input": {},
            "expectation": {"to_label": False},
            "inferred_labels": [{"labels": {"mode_confirm": "walk",
                "purpose_confirm": "exercise"}, "p": 0.9}]}
        ] * 3)
        test_exp_result = pd.concat([test_unlabeled_df, pd.DataFrame([
            {"mode_confirm": "walk", "purpose_confirm" : "exercise"}] * 3)], axis=1)
        pd.testing.assert_frame_equal(esdt.expand_finallabels(test_unlabeled_df),
            test_exp_result)

        # all green; labeled from scratch
        test_labeled_df = pd.DataFrame([{
            "user_input": {"mode_confirm": "bike", "purpose_confirm": "shopping"},
            "expectation": {"to_label": True}}] * 3)
        test_exp_result = pd.concat([test_labeled_df, pd.DataFrame([
            {"mode_confirm": "bike", "purpose_confirm" : "shopping"}] * 3)], axis=1)
        # logging.debug("expected result index = %s" % test_exp_result.index)
        # logging.debug("actual result index = %s" % esdt.expand_finallabels(test_labeled_df).index)
        pd.testing.assert_frame_equal(esdt.expand_finallabels(test_labeled_df),
            test_exp_result)

        # all green; labeled from high confidence trips; user input is retained
        test_labeled_df = pd.DataFrame([{
            "user_input": {"mode_confirm": "bike", "purpose_confirm": "shopping"},
            "expectation": {"to_label": False},
            "inferred_labels": [{"labels": {"mode_confirm": "walk",
                "purpose_confirm": "exercise"}, "p": 0.9}]}
        ] * 3)
        test_exp_result = pd.concat([test_labeled_df, pd.DataFrame([
            {"mode_confirm": "bike", "purpose_confirm" : "shopping"}] * 3)], axis=1)
        logging.debug(test_exp_result)
        pd.testing.assert_frame_equal(esdt.expand_finallabels(test_labeled_df),
            test_exp_result)

        # all green; labeled from low confidence trips; user input is retained
        test_labeled_df = pd.DataFrame([{
            "user_input": {"mode_confirm": "bike", "purpose_confirm": "shopping"},
            "expectation": {"to_label": True},
            "inferred_labels": [{"labels": {"mode_confirm": "walk",
                "purpose_confirm": "exercise"}, "p": 0.9}]}
        ] * 3)
        test_exp_result = pd.concat([test_labeled_df, pd.DataFrame([
            {"mode_confirm": "bike", "purpose_confirm" : "shopping"}] * 3)], axis=1)
        logging.debug(test_exp_result)
        pd.testing.assert_frame_equal(esdt.expand_finallabels(test_labeled_df),
            test_exp_result)

        # mixed labeled; three rows user input, three rows high confidence, three rows
        # low confidence, three rows unlabeled. additional columns added but
        # with some N/A
        test_mixed_df = pd.DataFrame(
            [{"user_input": {"mode_confirm": "bike", "purpose_confirm": "shopping"},
            "expectation": {"to_label": True}}] * 3 +
            [{"user_input": {}, "expectation": {"to_label": False},
            "inferred_labels":
                [{"labels": {"mode_confirm": "bike", "purpose_confirm": "shopping"}, "p": 0.1},
                {"labels": {"mode_confirm": "walk", "purpose_confirm": "exercise"}, "p": 0.9}]
            }] * 3 +
            [{"user_input": {}, "expectation": {"to_label": True},
            "inferred_labels":
                [{"labels": {"mode_confirm": "bike", "purpose_confirm": "shopping"}, "p": 0.2},
                {"labels": {"mode_confirm": "walk", "purpose_confirm": "exercise"}, "p": 0.4},
                {"labels": {"mode_confirm": "drove_alone", "purpose_confirm": "work"}, "p": 0.4}]
            }] * 3 +
            [{"user_input": {}, "expectation": {"to_label": True}}] * 3)
        result_df = pd.concat([test_mixed_df, pd.DataFrame(
        # Three green labels
            [{"mode_confirm": "bike", "purpose_confirm" : "shopping"}] * 3 +
        # Three high confidence yellow labels
            [{"mode_confirm": "walk", "purpose_confirm" : "exercise"}] * 3 +
        # Three low confidence yellow labels
            [{}] * 3 +
        # Three red labels
            [{}] * 3)], axis=1)
        actual_result = esdt.expand_finallabels(test_mixed_df)
        pd.testing.assert_frame_equal(actual_result, result_df)
        logging.debug(actual_result[["mode_confirm", "purpose_confirm"]])
        # The first three entries are bike
        self.assertEqual(actual_result.loc[0:2, "mode_confirm"].to_list(),
            ["bike"] * 3)
        # The next three entries are walk
        self.assertEqual(actual_result.loc[3:5, "mode_confirm"].to_list(),
            ["walk"] * 3)
        # The last six entries are N/A since they are red or low confidence
        # yellow labels
        self.assertTrue(pd.isna(actual_result.loc[6:12, "mode_confirm"]).all())

        # mixed labeled with different columns; additional columns added but with some N/A
        test_mixed_df = pd.DataFrame(
            [{"user_input": {"mode_confirm": "bike", "purpose_confirm": "shopping"},
            "expectation": {"to_label": True}}] * 3 +
            [{"user_input": {},
            "expectation": {"to_label": False},
            "inferred_labels": [{"labels": {"mode_confirm": "bike", "purpose_confirm": "shopping", "replaced_mode": "running"}, "p": 0.9}]
            }] * 3 +
            [{"user_input": {}, "expectation": {"to_label": True}}] * 3)
        result_df = pd.concat([test_mixed_df, pd.DataFrame(
        # Three partially expanded entries
            [{"mode_confirm": "bike", "purpose_confirm" : "shopping"}] * 3 +
        # Three fully expanded entries
            [{"mode_confirm": "bike", "purpose_confirm" : "shopping", "replaced_mode": "running"}] * 3 +
        # Three partial entries
            [{}] * 3)], axis=1)
        actual_result = esdt.expand_finallabels(test_mixed_df)
        pd.testing.assert_frame_equal(actual_result, result_df)
        # The first three entries have N/A replaced mode
        logging.debug(pd.isna(actual_result.loc[:2, "replaced_mode"]))
        self.assertTrue(pd.isna(actual_result.loc[:2, "replaced_mode"]).all())
        # The middle three entries have "running" replaced mode
        logging.debug(actual_result.loc[3:5, "replaced_mode"])
        self.assertEqual(actual_result.loc[3:5, "replaced_mode"].to_list(), ["running"] * 3)
        # The last three entries have N/A for all expanded values
        logging.debug(pd.isna(actual_result.loc[6:,["mode_confirm", "purpose_confirm", "replaced_mode"]]))
        self.assertTrue(pd.isna(actual_result.loc[6:,["mode_confirm", "purpose_confirm", "replaced_mode"]].to_numpy().flatten()).all())



if __name__ == '__main__':
    import emission.tests.common as etc
    etc.configLogging()
    unittest.main()
