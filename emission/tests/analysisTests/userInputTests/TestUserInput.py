import unittest
import logging
import json
import bson.json_util as bju
import argparse
import numpy as np

# Our imports
import emission.core.get_database as edb
import emission.core.wrapper.localdate as ecwl
import emission.core.wrapper.entry as ecwe
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

    def compare_trip_result(self, result_dicts, expect_dicts):
        # This is basically a bunch of asserts to ensure that the timeline is as
        # expected. We are not using a recursive diff because things like the IDs
        # will change from run to run. Instead, I pick out a bunch of important
        # things that are highly user visible
        # Since this is deterministic, we can also include things that are not that user visible :)
        result = [ecwe.Entry(r) for r in result_dicts]
        expect = [ecwe.Entry(e) for e in expect_dicts]

        for rt, et in zip(result, expect):
            logging.debug("Comparing %s -> %s with %s -> %s" %
                          (rt.data.start_fmt_time, rt.data.end_fmt_time,
                           et.data.start_fmt_time, et.data.end_fmt_time))
        self.assertEqual(len(result), len(expect))
        for rt, et in zip(result, expect):
            logging.debug("======= Comparing trip =========")
            logging.debug(json.dumps(rt, indent=4, default=bju.default))
            logging.debug(json.dumps(et, indent=4, default=bju.default))
            # Highly user visible
            self.assertEqual(rt.data["user_input"], et.data["user_input"])
            # self.assertEqual(rt.data.inferred_primary_mode, et.data.inferred_primary_mode)
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

    def checkConfirmedTripsAndSections(self, dataFile, ld, preload=False):
        with open(dataFile+".ground_truth") as gfp:
            ground_truth = json.load(gfp, object_hook=bju.object_hook)

        etc.setupRealExample(self, dataFile)
        if (preload):
            self.entries = json.load(open(dataFile+".user_inputs"), object_hook = bju.object_hook)
            etc.setupRealExampleWithEntries(self)
        etc.runIntakePipeline(self.testUUID)
        if (not preload):
            self.entries = json.load(open(dataFile+".user_inputs"), object_hook = bju.object_hook)
            etc.setupRealExampleWithEntries(self)
            etc.runIntakePipeline(self.testUUID)
        ts = esta.TimeSeries.get_time_series(self.testUUID)
        confirmed_trips = list(ts.find_entries(["analysis/confirmed_trip"], None))
        with open(dataFile+".expected_confirmed_trips") as dect:
            expected_confirmed_trips = json.load(dect, object_hook = bju.object_hook)
            self.compare_trip_result(confirmed_trips, expected_confirmed_trips)

#         confirmed_sections = ts.find_entries(["analysis/confirmed_section"],
#             estc.TimeComponentQuery("data.local_dt", ld, ld))
#         with open(dataFile+".expected_confirmed_sections") as dect:
#             expected_confirmed_sections = json.load(dect, object_hook = bju.object_hook)
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

#     def testJun20Postload(self):
#         # Same as testJun20Preload, except that the user input arrives after the
#         # pipeline is run for the first time, and the matching happens on the
#         # next pipeline run
#         dataFile = "emission/tests/data/real_examples/shankari_2016-06-20"
#         ld = ecwl.LocalDate({'year': 2016, 'month': 6, 'day': 20})
#         self.checkConfirmedTripsAndSections(dataFile, ld, preload=False)

if __name__ == '__main__':
    etc.configLogging()

    parser = argparse.ArgumentParser()
    parser.add_argument("--algo_change",
        help="modifications to the algorithm", action="store_true")
    unittest.main()
