import unittest
import logging
import json
import emission.storage.json_wrappers as esj
import argparse
import numpy as np
import os

# Our imports
import emission.core.get_database as edb
import emission.core.wrapper.localdate as ecwl
import emission.core.wrapper.entry as ecwe
import emission.core.wrapper.user as ecwu
import emission.storage.timeseries.abstract_timeseries as esta
import emission.storage.timeseries.tcquery as estc

import emission.net.usercache.abstract_usercache_handler as enuah
import emission.analysis.plotting.geojson.geojson_feature_converter as gfc
import emission.storage.timeseries.tcquery as estt
import emission.core.common as ecc

# Test imports
import emission.tests.common as etc

class TestUserInput(unittest.TestCase):
    def setUp(self):
        # Thanks to M&J for the number!
        np.random.seed(61297777)
        logging.info("setUp complete")

    def tearDown(self):
        if os.environ.get("SKIP_TEARDOWN", False):
            logging.info("SKIP_TEARDOWN = true, not clearing related databases")
            ecwu.User.registerWithUUID("automated_tests", self.testUUID)
        else:
            logging.debug("Clearing related databases for %s" % self.testUUID)
            # Clear the database only if it is not an evaluation run
            # A testing run validates that nothing has changed
            # An evaluation run compares to different algorithm implementations
            # to determine whether to switch to a new implementation
            if not hasattr(self, "evaluation") or not self.evaluation:
                self.clearRelatedDb()
            if hasattr(self, "analysis_conf_path"):
                os.remove(self.analysis_conf_path)
            logging.info("tearDown complete")

    def clearRelatedDb(self):
        logging.info("Timeseries delete result %s" % edb.get_timeseries_db().delete_many({"user_id": self.testUUID}).raw_result)
        logging.info("Analysis delete result %s" % edb.get_analysis_timeseries_db().delete_many({"user_id": self.testUUID}).raw_result)
        logging.info("Usercache delete result %s" % edb.get_usercache_db().delete_many({"user_id": self.testUUID}).raw_result)

    def compare_confirmed_objs_result(self, result_dicts, expect_dicts, manual_keys = None):
        # This is basically a bunch of asserts to ensure that the timeline is as
        # expected. We are not using a recursive diff because things like the IDs
        # will change from run to run. Instead, I pick out a bunch of important
        # things that are highly user visible
        # Since this is deterministic, we can also include things that are not that user visible :)
        result = [ecwe.Entry(r) for r in result_dicts]
        expect = [ecwe.Entry(e) for e in expect_dicts]

        for rt, et in zip(result, expect):
            fmt_start = lambda x: x.data.start_fmt_time if hasattr(x.data, "start_fmt_time") else x.data.enter_fmt_time
            fmt_end = lambda x: x.data.end_fmt_time if hasattr(x.data, "end_fmt_time") else x.data.exit_fmt_time
            logging.debug("Comparing %s -> %s with %s -> %s" %
                          (fmt_start(rt), fmt_end(rt),
                           fmt_start(et), fmt_end(et)))
        self.assertEqual(len(result), len(expect))
        for rt, et in zip(result, expect):
            logging.debug("======= Comparing trip =========")
            logging.debug(json.dumps(rt, indent=4, default=esj.wrapped_default))
            logging.debug(json.dumps(et, indent=4, default=esj.wrapped_default))
            # Highly user visible
            if manual_keys:
                for key in manual_keys:
                    # Compare result user inputs to expected user inputs
                    if key in rt.data['user_input']:
                        self.assertEqual(rt.data['user_input'][key]['data'], et.data['user_input'][key]['data'])
                    else:
                        self.assertFalse(key in et.data['user_input'])

                # Compare result additions to expected additions
                self.assertEqual(len(rt.data['additions']), len(et.data['additions']))
                for ra, ea in zip(rt.data['additions'], et.data['additions']):
                    self.assertEqual(ra['data'], ea['data'])
            else:
                self.assertEqual(rt.data["user_input"], et.data["user_input"])

            self.assertEqual(rt.data.keys(), et.data.keys())
            if "inferred_section_summary" in rt.data:
                self.assertEqual(rt.data["inferred_section_summary"], et.data["inferred_section_summary"])
            if "cleaned_section_summary" in et.data:
                # Check keys match
                self.assertEqual(rt.data["cleaned_section_summary"].keys(), et.data["cleaned_section_summary"].keys())
                
                # Handle distance values with assertAlmostEqual for floating point comparison
                if "distance" in rt.data["cleaned_section_summary"]:
                    self.assertEqual(rt.data["cleaned_section_summary"]["distance"].keys(), 
                                    et.data["cleaned_section_summary"]["distance"].keys())
                    for mode in rt.data["cleaned_section_summary"]["distance"]:
                        self.assertAlmostEqual(rt.data["cleaned_section_summary"]["distance"][mode],
                                              et.data["cleaned_section_summary"]["distance"][mode],
                                              delta=1e-5)
                
                # For all other keys, use exact comparison
                for key in [k for k in rt.data["cleaned_section_summary"].keys() if k != "distance"]:
                    self.assertEqual(rt.data["cleaned_section_summary"][key], et.data["cleaned_section_summary"][key])
            
            if 'ble_sensed_summary' in et.data:
                self.assertEqual(rt.data["ble_sensed_summary"], et.data["ble_sensed_summary"])
            logging.debug(20 * "=")

    def compare_section_result(self, result, expect):
        # This is basically a bunch of asserts to ensure that the timeline is as
        # expected. We are not using a recursive diff because things like the IDs
        # will change from run to run. Instead, I pick out a bunch of important
        # things that are highly user visible
        # Since this is deterministic, we can also include things that are not that user visible :)

        for rt, et in zip(result, expect):
            logging.debug("Comparing %s -> %s with %s -> %s" %
                          (rt.start_fmt_time, rt.end_fmt_time,
                           et.start_fmt_time, et.end_fmt_time))
        self.assertEqual(len(result), len(expect))
        for rt, et in zip(result, expect):
            logging.debug("======= Comparing section =========")
            # Highly user visible
            self.assertEqual(rt.inferred_mode, et.inferred_mode)
            self.assertEqual(rt.confirmed_mode, et.confirmed_mode)
            self.assertEqual(rt.analysis_mode, et.analysis_mode)
            self.assertEqual(rt.display_mode, et.display_mode)
            logging.debug(20 * "=")

    def checkConfirmedTripsAndSections(self, dataFile, ld, preload=False, trip_user_inputs=False, place_user_inputs=False):
        ct_suffix = "".join(".manual_" + k for k in trip_user_inputs) if trip_user_inputs else ""
        cp_suffix = "".join(".manual_" + k for k in place_user_inputs) if place_user_inputs else ""
        logging.debug("Checking confirmed entries against trip suffix %s and place suffix %s" % (ct_suffix, cp_suffix))
        
        with open(dataFile+".ground_truth") as gfp:
            ground_truth = json.load(gfp, object_hook=esj.wrapped_object_hook)

        etc.setupRealExample(self, dataFile)
        if (preload):
            self.entries = json.load(open(dataFile+".user_inputs"+ct_suffix+cp_suffix), object_hook = esj.wrapped_object_hook)
            etc.setupRealExampleWithEntries(self)
        etc.runIntakePipeline(self.testUUID)
        if (not preload):
            self.entries = json.load(open(dataFile+".user_inputs"+ct_suffix+cp_suffix), object_hook = esj.wrapped_object_hook)
            etc.setupRealExampleWithEntries(self)
            etc.runIntakePipeline(self.testUUID)
        ts = esta.TimeSeries.get_time_series(self.testUUID)
        confirmed_trips = ts.find_entries(["analysis/confirmed_trip"], None)
        confirmed_places = ts.find_entries(["analysis/confirmed_place"], None)

        with open(dataFile+".expected_confirmed_trips"+ct_suffix) as dect:
            expected_confirmed_trips = json.load(dect, object_hook = esj.wrapped_object_hook)
            self.compare_confirmed_objs_result(confirmed_trips, expected_confirmed_trips, manual_keys=["trip_user_input"] if trip_user_inputs else None) 
        with open(dataFile+".expected_confirmed_places"+cp_suffix) as dect:
            expected_confirmed_places = json.load(dect, object_hook = esj.wrapped_object_hook)
            self.compare_confirmed_objs_result(confirmed_places, expected_confirmed_places, manual_keys=["place_user_input"] if place_user_inputs else None)

#         confirmed_sections = ts.find_entries(["analysis/confirmed_section"],
#             estc.TimeComponentQuery("data.local_dt", ld, ld))
#         with open(dataFile+".expected_confirmed_sections") as dect:
#             expected_confirmed_sections = json.load(dect, object_hook = esj.wrapped_object_hook)
#             self.compare_section_result(confirmed_sections, expected_confirmed_sections)


    def testJun20Preload(self):
        # Tests matching where user input is stored before the pipeline is run
        # - trips with a single match and an exact start/end match (easy case, user input on cleaned trip)
        # ---- Trip to karate
        # - trips with a single match and an start/end match after the cleaned start/end (user input on draft trip)
        # ---- First trip to library
        # - trips with multiple matches (pick most recent)
        # ---- Trip from karate (in draft mode)
        # ---- Trip back from library in the afternoon (in final mode)
        # - user input with no matching trip (should be ignored)
        # - trips with no matches
        # ---- Trip back from library in the morning
        # ---- Trip to library in the afternoon
        # - trip that was first set in draft mode and then overriden in cleaned mode
        # ---- Trip to karate
        dataFile = "emission/tests/data/real_examples/shankari_2016-06-20"
        ld = ecwl.LocalDate({'year': 2016, 'month': 6, 'day': 20})
        self.checkConfirmedTripsAndSections(dataFile, ld, preload=True)

    def testJun20Postload(self):
        # Same as testJun20Preload, except that the user input arrives after the
        # pipeline is run for the first time, and the matching happens on the
        # next pipeline run
        dataFile = "emission/tests/data/real_examples/shankari_2016-06-20"
        ld = ecwl.LocalDate({'year': 2016, 'month': 6, 'day': 20})
        self.checkConfirmedTripsAndSections(dataFile, ld, preload=False)

    def testTripUserInput(self):
        dataFile = "emission/tests/data/real_examples/shankari_2016-06-20"
        ld = ecwl.LocalDate({'year': 2016, 'month': 6, 'day': 20})
        self.checkConfirmedTripsAndSections(dataFile, ld, preload=True,
                                            trip_user_inputs=["trip_user_input"])

    def testTripAndPlaceAdditions(self):
        # > shankari_2016-06-20.user_inputs.manual_trip_addition_input.manual_place_addition_input
        # This will load trip-level and place-level additions from the above file
        # This includes a few DELETED entries, which should not be matched
        dataFile = "emission/tests/data/real_examples/shankari_2016-06-20"
        ld = ecwl.LocalDate({'year': 2016, 'month': 6, 'day': 20})
        self.checkConfirmedTripsAndSections(dataFile, ld, preload=True,
                                            trip_user_inputs=["trip_addition_input"],
                                            place_user_inputs=["place_addition_input"])

if __name__ == '__main__':
    etc.configLogging()

    parser = argparse.ArgumentParser()
    parser.add_argument("--algo_change",
        help="modifications to the algorithm", action="store_true")
    unittest.main()
