from __future__ import unicode_literals
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import
# This test compares the output of the intake pipeline
# with known ground truth output.
# The way to add a new test is:
# - load the test timeline
# $ ./e-mission-py.bash bin/debug/load_timeline_for_day_and_user.py emission/tests/data/real_examples/iphone_2016-02-22 test-iphone-feb-22
# ...
# Loading file emission/tests/data/real_examples/iphone_2016-02-22
# After registration, test-iphone-feb-22 -> 349b4f21-7cd2-4ac6-8786-ea69142c2238
#
# Note that there is some randomness in the tests, due to
# a56adddc5dc8c94cbe98964aafb17df3bc3f724c, so we need to use a random seed
# The tests use a seed of 61297777 - if the intake pipeline is being run to
# generate ground truth, the line setting the seed in the intake pipeline
# needs to be re-instituted.

# - run the intake pipeline
# $ ./e-mission-py.bash bin/debug/intake_single_user.py -e test-iphone-feb-22
# - log in via the phone and check that all is well
# - save the ground truth
# $ ./e-mission-py.bash bin/debug/save_ground_truth.py -e test-iphone-feb-22 2016-02-22 /tmp/iphone_2016-02-22.ground_truth
# Copy it back and add the test to this file
# $ mv /tmp/iphone_2016-02-22.ground_truth emission/tests/data/real_examples

from future import standard_library
standard_library.install_aliases()
from builtins import zip
from builtins import *
import unittest
import logging
import json
import bson.json_util as bju
import attrdict as ad
import arrow
import numpy as np
import os
import os.path
import argparse

# Our imports
import emission.core.get_database as edb
import emission.core.wrapper.localdate as ecwl

import emission.net.usercache.abstract_usercache_handler as enuah
import emission.analysis.plotting.geojson.geojson_feature_converter as gfc
import emission.storage.timeseries.tcquery as estt
import emission.core.common as ecc

# Test imports
import emission.tests.common as etc

class TestPipelineRealData(unittest.TestCase):
    def setUp(self):
        # Thanks to M&J for the number!
        np.random.seed(61297777)
        self.analysis_conf_path = \
            etc.set_analysis_config("analysis.result.section.key", "analysis/cleaned_section")
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

    def compare_result(self, result, expect):
        # This is basically a bunch of asserts to ensure that the timeline is as
        # expected. We are not using a recursive diff because things like the IDs
        # will change from run to run. Instead, I pick out a bunch of important
        # things that are highly user visible
        # Since this is deterministic, we can also include things that are not that user visible :)

        for rt, et in zip(result, expect):
            logging.debug("Comparing %s -> %s with %s -> %s" %
                          (rt.properties.start_fmt_time, rt.properties.end_fmt_time,
                           et.properties.start_fmt_time, et.properties.end_fmt_time))
        self.assertEqual(len(result), len(expect))
        for rt, et in zip(result, expect):
            logging.debug("======= Comparing trip =========")
            logging.debug(json.dumps(rt.properties, indent=4, default=bju.default))
            logging.debug(json.dumps(et.properties, indent=4, default=bju.default))
            # Highly user visible
            self.assertEqual(rt.properties.start_ts, et.properties.start_ts)
            self.assertEqual(rt.properties.end_ts, et.properties.end_ts)
            self.assertEqual(rt.properties.start_loc, et.properties.start_loc)
            self.assertEqual(rt.properties.end_loc, et.properties.end_loc)
            self.assertAlmostEqual(rt.properties.distance, et.properties.distance, places=2)
            self.assertEqual(len(rt.features), len(et.features))

            for rs, es in zip(rt.features, et.features):
                logging.debug("------- Comparing trip feature ---------")
                logging.debug(json.dumps(rs, indent=4, default=bju.default))
                logging.debug(json.dumps(es, indent=4, default=bju.default))
                self.assertEqual(rs.type, es.type)
                if rs.type == "Feature":
                    # The first place will not have an enter time, so we can't check it
                    if 'enter_fmt_time' not in rs.properties:
                        self.assertNotIn("enter_fmt_time", es.properties)
                    else:
                        self.assertEqual(rs.properties.enter_fmt_time, es.properties.enter_fmt_time)

                    # Similarly, the last place will not have an exit time, so we can't check it
                    if 'exit_fmt_time' not in rs.properties:
                        self.assertNotIn("exit_fmt_time", es.properties)
                    else:
                        self.assertEqual(rs.properties.exit_fmt_time, es.properties.exit_fmt_time)
                    self.assertEqual(rs.properties.feature_type, es.properties.feature_type)
                else:
                    self.assertEqual(rs.type, "FeatureCollection")
                    self.assertEqual(rs.features[0].properties.start_fmt_time, es.features[0].properties.start_fmt_time)
                    self.assertEqual(rs.features[0].properties.end_fmt_time, es.features[0].properties.end_fmt_time)
                    self.assertEqual(rs.features[0].properties.sensed_mode, es.features[0].properties.sensed_mode)
                    self.assertEqual(len(rs.features[0].properties.speeds), len(es.features[0].properties.speeds))
                    self.assertEqual(len(rs.features[0].geometry.coordinates), len(es.features[0].geometry.coordinates))
                logging.debug(20 * "-")
            logging.debug(20 * "=")

    def compare_approx_result(self, result, expect, distance_fuzz, time_fuzz):
        # This is basically a bunch of asserts to ensure that the timeline is as
        # expected. We are not using a recursive diff because things like the IDs
        # will change from run to run. Instead, I pick out a bunch of important
        # things that are highly user visible
        # Since this is deterministic, we can also include things that are not that user visible :)

        for rt, et in zip(result, expect):
            logging.debug("Comparing %s -> %s with %s -> %s" %
                          (rt.properties.start_fmt_time, rt.properties.end_fmt_time,
                           et.properties.start_fmt_time, et.properties.end_fmt_time))
        self.assertEqual(len(result), len(expect))
        for rt, et in zip(result, expect):
            logging.debug("======= Comparing trip =========")
            logging.debug(json.dumps(rt.properties, indent=4, default=bju.default))
            logging.debug(json.dumps(et.properties, indent=4, default=bju.default))
            # Highly user visible
            self.assertAlmostEqual(rt.properties.start_ts, et.properties.start_ts, delta=time_fuzz)
            self.assertAlmostEqual(rt.properties.end_ts, et.properties.end_ts, delta=time_fuzz)
            self.assertLessEqual(ecc.calDistance(rt.properties.start_loc.coordinates, et.properties.start_loc.coordinates), distance_fuzz)
            self.assertLessEqual(ecc.calDistance(rt.properties.end_loc.coordinates, et.properties.end_loc.coordinates), distance_fuzz)
            self.assertAlmostEqual(rt.properties.distance, et.properties.distance, delta=distance_fuzz)
            self.assertEqual(len(rt.features), len(et.features))

            for rs, es in zip(rt.features, et.features):
                logging.debug("------- Comparing trip feature ---------")
                logging.debug(json.dumps(rs, indent=4, default=bju.default))
                logging.debug(json.dumps(es, indent=4, default=bju.default))
                self.assertEqual(rs.type, es.type)
                if rs.type == "Feature":
                    # The first place will not have an enter time, so we can't check it
                    if 'enter_fmt_time' not in rs.properties:
                        self.assertNotIn("enter_fmt_time", es.properties)
                    else:
                        self.assertAlmostEqual(rs.properties.enter_ts, es.properties.enter_ts, delta=time_fuzz)

                    # Similarly, the last place will not have an exit time, so we can't check it
                    if 'exit_fmt_time' not in rs.properties:
                        self.assertNotIn("exit_fmt_time", es.properties)
                    else:
                        self.assertAlmostEqual(rs.properties.exit_ts, es.properties.exit_ts, delta=time_fuzz)
                    self.assertEqual(rs.properties.feature_type, es.properties.feature_type)
                else:
                    self.assertEqual(rs.type, "FeatureCollection")
                    self.assertAlmostEqual(rs.features[0].properties.start_ts, es.features[0].properties.start_ts, delta=time_fuzz)
                    self.assertAlmostEqual(rs.features[0].properties.end_ts, es.features[0].properties.end_ts, delta=time_fuzz)
                    self.assertEqual(rs.features[0].properties.sensed_mode, es.features[0].properties.sensed_mode)
                    # Fuzz for resampled data as well
                    # https://github.com/e-mission/e-mission-server/issues/288#issuecomment-242450106
                    self.assertAlmostEqual(len(rs.features[0].properties.speeds), len(es.features[0].properties.speeds), delta=2)
                    self.assertAlmostEqual(len(rs.features[0].geometry.coordinates), len(es.features[0].geometry.coordinates), delta=2)
                logging.debug(20 * "-")
            logging.debug(20 * "=")

    def persistGroundTruthIfNeeded(self, api_result, dataFile, ld, cacheKey):
        if hasattr(self, "persistence") and self.persistence:
            savedFn = "/tmp/"+os.path.basename(dataFile)+".ground_truth"
            logging.info("Persisting ground truth to "+savedFn)
            with open(savedFn, "w") as gofp:
                wrapped_gt = {
                    "data": api_result,
                    "metadata": {
                        "key": cacheKey,
                        "type": "document",
                        "write_ts": arrow.now().timestamp
                    }
                }
                json.dump(wrapped_gt, gofp, indent=4, default=bju.default)

    def standardMatchDataGroundTruth(self, dataFile, ld, cacheKey):
        with open(dataFile+".ground_truth") as gfp:
            ground_truth = json.load(gfp, object_hook=bju.object_hook)

        etc.setupRealExample(self, dataFile)
        etc.runIntakePipeline(self.testUUID)
        # runIntakePipeline does not run the common trips, habitica or store views to cache
        # So let's manually store to the cache
        # tc_query = estt.TimeComponentQuery("data.star_local_dt", ld, ld)
        # enuah.UserCacheHandler.getUserCacheHandler(self.testUUID).storeTimelineToCache(tc_query)

        # cached_result = edb.get_usercache_db().find_one({'user_id': self.testUUID,
        #                                                  "metadata.key": cacheKey})
        api_result = gfc.get_geojson_for_dt(self.testUUID, ld, ld)
        self.persistGroundTruthIfNeeded(api_result, dataFile, ld, cacheKey)

        # self.compare_result(cached_result, ground_truth)
        self.compare_result(ad.AttrDict({'result': api_result}).result,
                            ad.AttrDict(ground_truth).data)

    def testJun20(self):
        # This is a fairly straightforward day. Tests mainly:
        # - ordering of trips
        # - handling repeated location entries with different write timestamps
        # We have two identical location points with ts = 1466436483.395 and write_ts = 1466436496.4, 1466436497.047
        dataFile = "emission/tests/data/real_examples/shankari_2016-06-20"
        ld = ecwl.LocalDate({'year': 2016, 'month': 6, 'day': 20})
        cacheKey = "diary/trips-2016-06-20"
        self.standardMatchDataGroundTruth(dataFile, ld, cacheKey)

    def testJun21(self):
        # This is a more complex day. Tests:
        # PR #357 (spurious trip at 14:00 should be segmented and skipped)
        # PR #358 (trip back from bella's house at 16:00)

        dataFile = "emission/tests/data/real_examples/shankari_2016-06-21"
        ld = ecwl.LocalDate({'year': 2016, 'month': 6, 'day': 21})
        cacheKey = "diary/trips-2016-06-21"
        self.standardMatchDataGroundTruth(dataFile, ld, cacheKey)

    def testAug10(self):
        # This is a more complex day. Tests:
        # PR #302 (trip to optometrist)
        # PR #352 (split optometrist trip)

        dataFile = "emission/tests/data/real_examples/shankari_2016-08-10"
        ld = ecwl.LocalDate({'year': 2016, 'month': 8, 'day': 10})
        cacheKey = "diary/trips-2016-08-10"
        self.standardMatchDataGroundTruth(dataFile, ld, cacheKey)

    def testAug11(self):
        # This is a more complex day. Tests:
        # PR #352 (should not split trip to Oakland)
        # PR #348 (trip from station to OAK DOT)
        # PR #357 (trip to Radio Shack is complete and not truncated)
        # PR #345 (no cleaned trips are skipped)

        dataFile = "emission/tests/data/real_examples/shankari_2016-08-11"
        ld = ecwl.LocalDate({'year': 2016, 'month': 8, 'day': 11})
        cacheKey = "diary/trips-2016-08-11"
        self.standardMatchDataGroundTruth(dataFile, ld, cacheKey)

    def testFeb22ShortTripsDistance(self):
        dataFile = "emission/tests/data/real_examples/iphone_3_2016-02-22"
        start_ld = ecwl.LocalDate({'year': 2016, 'month': 2, 'day': 22})
        end_ld = ecwl.LocalDate({'year': 2016, 'month': 2, 'day': 22})
        cacheKey = "diary/trips-2016-02-22"
        self.standardMatchDataGroundTruth(dataFile, start_ld, cacheKey)

    def testAug27TooMuchExtrapolation(self):
        dataFile = "emission/tests/data/real_examples/shankari_2015-aug-27"
        start_ld = ecwl.LocalDate({'year': 2015, 'month': 8, 'day': 27})
        end_ld = ecwl.LocalDate({'year': 2015, 'month': 8, 'day': 27})
        cacheKey = "diary/trips-2015-08-27"
        self.standardMatchDataGroundTruth(dataFile, start_ld, cacheKey)

    def testAirTripToHawaii(self):
        dataFile = "emission/tests/data/real_examples/shankari_2016-07-27"
        start_ld = ecwl.LocalDate({'year': 2016, 'month': 7, 'day': 27})
        cacheKey = "diary/trips-2016-07-27"
        self.standardMatchDataGroundTruth(dataFile, start_ld, cacheKey)

    def testAirTripHawaiiEnd(self):
        dataFile = "emission/tests/data/real_examples/shankari_2016-08-04"
        start_ld = ecwl.LocalDate({'year': 2016, 'month': 8, 'day': 4})
        cacheKey = "diary/trips-2016-07-27"
        self.standardMatchDataGroundTruth(dataFile, start_ld, cacheKey)

    def testAirTripFromHawaii(self):
        dataFile = "emission/tests/data/real_examples/shankari_2016-08-05"
        start_ld = ecwl.LocalDate({'year': 2016, 'month': 8, 'day': 0o5})
        cacheKey = "diary/trips-2016-07-05"
        self.standardMatchDataGroundTruth(dataFile, start_ld, cacheKey)

    def testSunilShortTrips(self):
        dataFile = "emission/tests/data/real_examples/sunil_2016-07-27"
        start_ld = ecwl.LocalDate({'year': 2016, 'month': 7, 'day': 27})
        cacheKey = "diary/trips-2016-07-27"
        etc.setupRealExample(self, dataFile)
        etc.runIntakePipeline(self.testUUID)

        api_result = gfc.get_geojson_for_dt(self.testUUID, start_ld, start_ld)
        self.persistGroundTruthIfNeeded(api_result, dataFile, start_ld, cacheKey)
        # Although we process the day's data in two batches, we should get the same result
        self.assertEqual(api_result, [])

    def testGabeShortTrips(self):
        dataFile = "emission/tests/data/real_examples/gabe_2016-06-15"
        start_ld = ecwl.LocalDate({'year': 2016, 'month': 6, 'day': 15})
        cacheKey = "diary/trips-2016-06-15"
        self.standardMatchDataGroundTruth(dataFile, start_ld, cacheKey)

    def testJumpSmoothingSectionEnd(self):
        dataFile = "emission/tests/data/real_examples/shankari_2016-independence_day"
        start_ld = ecwl.LocalDate({'year': 2016, 'month': 8, 'day': 15})
        cacheKey = "diary/trips-2016-08-15"
        self.standardMatchDataGroundTruth(dataFile, start_ld, cacheKey)

    def testJumpSmoothingSectionsStraddle(self):
        dataFile = "emission/tests/data/real_examples/shankari_2016-independence_day_jump_straddle"
        start_ld = ecwl.LocalDate({'year': 2016, 'month': 8, 'day': 15})
        cacheKey = "diary/trips-2016-08-15"
        with open("emission/tests/data/real_examples/shankari_2016-independence_day.alt.ground_truth") as gfp:
            ground_truth = json.load(gfp, object_hook=bju.object_hook)

        etc.setupRealExample(self, dataFile)
        etc.runIntakePipeline(self.testUUID)

        api_result = gfc.get_geojson_for_dt(self.testUUID, start_ld, start_ld)
        self.persistGroundTruthIfNeeded(api_result, dataFile, start_ld, cacheKey)
        # Although we process the day's data in two batches, we should get the same result
        self.compare_result(ad.AttrDict({'result': api_result}).result,
                            ad.AttrDict(ground_truth).data)

    def testJumpSmoothingSectionStart(self):
        dataFile = "emission/tests/data/real_examples/shankari_2016-independence_day_jump_bus_start"
        start_ld = ecwl.LocalDate({'year': 2016, 'month': 8, 'day': 15})
        cacheKey = "diary/trips-2016-08-15"
        with open("emission/tests/data/real_examples/shankari_2016-independence_day.alt.ground_truth") as gfp:
            ground_truth = json.load(gfp, object_hook=bju.object_hook)

        etc.setupRealExample(self, dataFile)
        etc.runIntakePipeline(self.testUUID)

        api_result = gfc.get_geojson_for_dt(self.testUUID, start_ld, start_ld)
        self.persistGroundTruthIfNeeded(api_result, dataFile, start_ld, cacheKey)
        # Although we process the day's data in two batches, we should get the same result
        self.compare_result(ad.AttrDict({'result': api_result}).result,
                            ad.AttrDict(ground_truth).data)

    def testIndexLengthChange(self):
        # Test for 94f67b4848611fa01c4327a0fa0cab97c2247744
        dataFile = "emission/tests/data/real_examples/shankari_2015-08-23"
        start_ld = ecwl.LocalDate({'year': 2015, 'month': 8, 'day': 23})
        cacheKey = "diary/trips-2015-08-23"
        self.standardMatchDataGroundTruth(dataFile, start_ld, cacheKey)

    def testSquishedMismatchForUntrackedTime(self):
        # Test for a2c0ee4e3ceafa822425ceef299dcdb01c9b32c9
        dataFile = "emission/tests/data/real_examples/shankari_2015-07-22"
        start_ld = ecwl.LocalDate({'year': 2015, 'month': 7, 'day': 22})
        cacheKey = "diary/trips-2015-07-22"
        self.standardMatchDataGroundTruth(dataFile, start_ld, cacheKey)

    def testUnknownTrips(self):
        # Test for a2c0ee4e3ceafa822425ceef299dcdb01c9b32c9
        dataFile = "emission/tests/data/real_examples/shankari_2016-09-03"
        start_ld = ecwl.LocalDate({'year': 2016, 'month': 9, 'day': 3})
        cacheKey = "diary/trips-2016-09-03"
        self.standardMatchDataGroundTruth(dataFile, start_ld, cacheKey)

    def testIosJumpsAndUntrackedSquishing(self):
        # Test for a2c0ee4e3ceafa822425ceef299dcdb01c9b32c9
        dataFile = "emission/tests/data/real_examples/sunil_2016-07-20"
        start_ld = ecwl.LocalDate({'year': 2016, 'month': 7, 'day': 20})
        cacheKey = "diary/trips-2016-07-20"
        self.standardMatchDataGroundTruth(dataFile, start_ld, cacheKey)

    def testAug10MultiSyncEndDetected(self):
        # Re-run, but with multiple calls to sync data
        # This tests the effect of online versus offline analysis and segmentation with potentially partial data

        dataFile = "emission/tests/data/real_examples/shankari_2016-08-10"
        start_ld = ecwl.LocalDate({'year': 2016, 'month': 8, 'day': 9})
        end_ld = ecwl.LocalDate({'year': 2016, 'month': 8, 'day': 10})
        cacheKey = "diary/trips-2016-08-10"
        with open("emission/tests/data/real_examples/shankari_2016-08-910.ground_truth") as gtf:
            ground_truth = json.load(gtf,
                                 object_hook=bju.object_hook)

        logging.info("Before loading, timeseries db size = %s" % edb.get_timeseries_db().estimated_document_count())
        all_entries = None
        with open(dataFile) as secondfp:
            all_entries = json.load(secondfp, object_hook = bju.object_hook)
        ts_1030 = arrow.get("2016-08-10T10:30:00-07:00").timestamp
        logging.debug("ts_1030 = %s, converted back = %s" % (ts_1030, arrow.get(ts_1030).to("America/Los_Angeles")))
        before_1030_entries = [e for e in all_entries if ad.AttrDict(e).metadata.write_ts <= ts_1030]
        after_1030_entries = [e for e in all_entries if ad.AttrDict(e).metadata.write_ts > ts_1030]

        # First load all data from the 9th. Otherwise, the missed trip is the first trip,
        # and we don't set the last_ts_processed
        # See the code around "logging.debug("len(segmentation_points) == 0, early return")"
        etc.setupRealExample(self, "emission/tests/data/real_examples/shankari_2016-08-09")

        # Sync at 10:30 to capture all the points on the trip *to* the optometrist
        self.entries = before_1030_entries
        etc.setupRealExampleWithEntries(self)
        etc.runIntakePipeline(self.testUUID)
        api_result = gfc.get_geojson_for_dt(self.testUUID, start_ld, end_ld)

        # Then sync after 10:30
        self.entries = after_1030_entries
        etc.setupRealExampleWithEntries(self)
        etc.runIntakePipeline(self.testUUID)
        api_result = gfc.get_geojson_for_dt(self.testUUID, start_ld, end_ld)
        self.persistGroundTruthIfNeeded(api_result, dataFile, start_ld, cacheKey)

        # Although we process the day's data in two batches, we should get the same result
        self.compare_approx_result(ad.AttrDict({'result': api_result}).result,
                            ad.AttrDict(ground_truth).data, time_fuzz=60, distance_fuzz=100)

    def testFeb22MultiSyncEndDetected(self):
        # Re-run, but with multiple calls to sync data
        # This tests the effect of online versus offline analysis and segmentation with potentially partial data

        dataFile = "emission/tests/data/real_examples/iphone_2016-02-22"
        start_ld = ecwl.LocalDate({'year': 2016, 'month': 2, 'day': 22})
        end_ld = ecwl.LocalDate({'year': 2016, 'month': 2, 'day': 22})
        cacheKey = "diary/trips-2016-02-22"
        with open(dataFile+".ground_truth") as gtf:
            ground_truth = json.load(gtf, object_hook=bju.object_hook)

        logging.info("Before loading, timeseries db size = %s" % edb.get_timeseries_db().estimated_document_count())
        with open(dataFile) as df:
            all_entries = json.load(df, object_hook = bju.object_hook)
        # 18:01 because the transition was at 2016-02-22T18:00:09.623404-08:00, so right after
        # 18:00
        ts_1800 = arrow.get("2016-02-22T18:00:30-08:00").timestamp
        logging.debug("ts_1800 = %s, converted back = %s" % (ts_1800, arrow.get(ts_1800).to("America/Los_Angeles")))
        before_1800_entries = [e for e in all_entries if ad.AttrDict(e).metadata.write_ts <= ts_1800]
        after_1800_entries = [e for e in all_entries if ad.AttrDict(e).metadata.write_ts > ts_1800]

        # Sync at 18:00 to capture all the points on the trip *to* the optometrist
        etc.createAndFillUUID(self)
        self.entries = before_1800_entries
        etc.setupRealExampleWithEntries(self)
        etc.runIntakePipeline(self.testUUID)
        api_result = gfc.get_geojson_for_dt(self.testUUID, start_ld, end_ld)

        # Then sync after 18:00
        self.entries = after_1800_entries
        etc.setupRealExampleWithEntries(self)
        etc.runIntakePipeline(self.testUUID)
        api_result = gfc.get_geojson_for_dt(self.testUUID, start_ld, end_ld)
        self.persistGroundTruthIfNeeded(api_result, dataFile, start_ld, cacheKey)

        # Although we process the day's data in two batches, we should get the same result
        self.compare_approx_result(ad.AttrDict({'result': api_result}).result,
                                   ad.AttrDict(ground_truth).data, time_fuzz=60, distance_fuzz=100)

    def testAug10MultiSyncEndNotDetected(self):
        # Re-run, but with multiple calls to sync data
        # This tests the effect of online versus offline analysis and segmentation with potentially partial data

        dataFile = "emission/tests/data/real_examples/shankari_2016-08-10"
        start_ld = ecwl.LocalDate({'year': 2016, 'month': 8, 'day': 9})
        end_ld = ecwl.LocalDate({'year': 2016, 'month': 8, 'day': 10})
        cacheKey = "diary/trips-2016-08-10"
        with open("emission/tests/data/real_examples/shankari_2016-08-910.ground_truth") as gtf:
            ground_truth = json.load(gtf,
                                 object_hook=bju.object_hook)

        logging.info("Before loading, timeseries db size = %s" % edb.get_timeseries_db().estimated_document_count())
        with open(dataFile) as df:
            all_entries = json.load(df, object_hook = bju.object_hook)
        ts_1030 = arrow.get("2016-08-10T10:30:00-07:00").timestamp
        logging.debug("ts_1030 = %s, converted back = %s" % (ts_1030, arrow.get(ts_1030).to("America/Los_Angeles")))
        before_1030_entries = [e for e in all_entries if ad.AttrDict(e).metadata.write_ts <= ts_1030]
        after_1030_entries = [e for e in all_entries if ad.AttrDict(e).metadata.write_ts > ts_1030]

        # First load all data from the 9th. Otherwise, the missed trip is the first trip,
        # and we don't set the last_ts_processed
        # See the code around "logging.debug("len(segmentation_points) == 0, early return")"
        etc.setupRealExample(self, "emission/tests/data/real_examples/shankari_2016-08-09")

        # Sync at 10:30 to capture all the points on the trip *to* the optometrist
        # Skip the last few points to ensure that the trip end is skipped
        self.entries = before_1030_entries[0:-2]
        etc.setupRealExampleWithEntries(self)
        etc.runIntakePipeline(self.testUUID)
        api_result = gfc.get_geojson_for_dt(self.testUUID, start_ld, end_ld)

        # Then sync after 10:30
        self.entries = after_1030_entries
        etc.setupRealExampleWithEntries(self)
        etc.runIntakePipeline(self.testUUID)
        api_result = gfc.get_geojson_for_dt(self.testUUID, start_ld, end_ld)
        self.persistGroundTruthIfNeeded(api_result, dataFile, start_ld, cacheKey)

        # Although we process the day's data in two batches, we should get the same result
        self.compare_approx_result(ad.AttrDict({'result': api_result}).result,
                                   ad.AttrDict(ground_truth).data, time_fuzz=60, distance_fuzz=100)

    def testJul22SplitAroundReboot(self):
        dataFile_1 = "emission/tests/data/real_examples/shankari_2016-07-22"
        dataFile_2 = "emission/tests/data/real_examples/shankari_2016-07-25"
        start_ld_1 = ecwl.LocalDate({'year': 2016, 'month': 7, 'day': 22})
        start_ld_2 = ecwl.LocalDate({'year': 2016, 'month': 7, 'day': 25})
        cacheKey_1 = "diary/trips-2016-07-22"
        cacheKey_2 = "diary/trips-2016-07-25"
        with open(dataFile_1+".ground_truth") as gtf1:
            ground_truth_1 = json.load(gtf1, object_hook=bju.object_hook)
        with open(dataFile_2+".ground_truth") as gtf2:
            ground_truth_2 = json.load(gtf2, object_hook=bju.object_hook)

        etc.setupRealExample(self, dataFile_1)
        etc.runIntakePipeline(self.testUUID)
        with open(dataFile_2) as df2:
            self.entries = json.load(df2, object_hook = bju.object_hook)
        etc.setupRealExampleWithEntries(self)
        etc.runIntakePipeline(self.testUUID)

        api_result = gfc.get_geojson_for_dt(self.testUUID, start_ld_1, start_ld_1)
        self.persistGroundTruthIfNeeded(api_result, dataFile_1, start_ld_1, cacheKey_1)
        # Although we process the day's data in two batches, we should get the same result
        self.compare_result(ad.AttrDict({'result': api_result}).result,
                            ad.AttrDict(ground_truth_1).data)

        api_result = gfc.get_geojson_for_dt(self.testUUID, start_ld_2, start_ld_2)
        self.persistGroundTruthIfNeeded(api_result, dataFile_2, start_ld_2, cacheKey_2)
        # Although we process the day's data in two batches, we should get the same result
        self.compare_result(ad.AttrDict({'result': api_result}).result,
                            ad.AttrDict(ground_truth_2).data)

    def testFeb22MultiSyncEndNotDetected(self):
        # Re-run, but with multiple calls to sync data
        # This tests the effect of online versus offline analysis and segmentation with potentially partial data

        dataFile = "emission/tests/data/real_examples/iphone_2016-02-22"
        start_ld = ecwl.LocalDate({'year': 2016, 'month': 2, 'day': 22})
        end_ld = ecwl.LocalDate({'year': 2016, 'month': 2, 'day': 22})
        cacheKey = "diary/trips-2016-02-22"
        with open(dataFile+".ground_truth") as gtf:
            ground_truth = json.load(gtf, object_hook=bju.object_hook)

        logging.info("Before loading, timeseries db size = %s" % edb.get_timeseries_db().estimated_document_count())
        with open(dataFile) as df:
            all_entries = json.load(df, object_hook = bju.object_hook)
        # 18:01 because the transition was at 2016-02-22T18:00:09.623404-08:00, so right after
        # 18:00
        ts_1800 = arrow.get("2016-02-22T18:00:30-08:00").timestamp
        logging.debug("ts_1800 = %s, converted back = %s" % (ts_1800, arrow.get(ts_1800).to("America/Los_Angeles")))
        before_1800_entries = [e for e in all_entries if ad.AttrDict(e).metadata.write_ts <= ts_1800]
        after_1800_entries = [e for e in all_entries if ad.AttrDict(e).metadata.write_ts > ts_1800]

        # Sync at 18:00 to capture all the points on the trip *to* the optometrist
        # Skip the last few points to ensure that the trip end is skipped
        etc.createAndFillUUID(self)
        self.entries = before_1800_entries[0:-2]
        etc.setupRealExampleWithEntries(self)
        etc.runIntakePipeline(self.testUUID)
        api_result = gfc.get_geojson_for_dt(self.testUUID, start_ld, end_ld)

        # Then sync after 18:00
        self.entries = after_1800_entries
        etc.setupRealExampleWithEntries(self)
        etc.runIntakePipeline(self.testUUID)
        api_result = gfc.get_geojson_for_dt(self.testUUID, start_ld, end_ld)
        self.persistGroundTruthIfNeeded(api_result, dataFile, start_ld, cacheKey)

        # Although we process the day's data in two batches, we should get the same result
        self.compare_approx_result(ad.AttrDict({'result': api_result}).result,
                                   ad.AttrDict(ground_truth).data, time_fuzz=60, distance_fuzz=100)

    def testOct07MultiSyncSpuriousEndDetected(self):
        # Re-run, but with multiple calls to sync data
        # This tests the effect of online versus offline analysis and segmentation with potentially partial data

        dataFile = "emission/tests/data/real_examples/issue_436_assertion_error"
        start_ld = ecwl.LocalDate({'year': 2016, 'month': 10, 'day': 0o7})
        end_ld = ecwl.LocalDate({'year': 2016, 'month': 10, 'day': 0o7})
        cacheKey = "diary/trips-2016-10-07"
        with open(dataFile+".ground_truth") as gtf:
            ground_truth = json.load(gtf, object_hook=bju.object_hook)

        logging.info("Before loading, timeseries db size = %s" % edb.get_timeseries_db().estimated_document_count())
        with open(dataFile) as df:
            all_entries = json.load(df, object_hook = bju.object_hook)
        # 18:01 because the transition was at 2016-02-22T18:00:09.623404-08:00, so right after
        # 18:00
        ts_1800 = arrow.get("2016-10-07T18:33:11-07:00").timestamp
        logging.debug("ts_1800 = %s, converted back = %s" % (ts_1800, arrow.get(ts_1800).to("America/Los_Angeles")))
        before_1800_entries = [e for e in all_entries if ad.AttrDict(e).metadata.write_ts <= ts_1800]
        after_1800_entries = [e for e in all_entries if ad.AttrDict(e).metadata.write_ts > ts_1800]

        # Sync at 18:00 to capture all the points on the trip *to* the optometrist
        # Skip the last few points to ensure that the trip end is skipped
        etc.createAndFillUUID(self)
        self.entries = before_1800_entries
        etc.setupRealExampleWithEntries(self)
        etc.runIntakePipeline(self.testUUID)
        api_result = gfc.get_geojson_for_dt(self.testUUID, start_ld, end_ld)

        # Then sync after 18:00
        self.entries = after_1800_entries
        etc.setupRealExampleWithEntries(self)
        etc.runIntakePipeline(self.testUUID)
        api_result = gfc.get_geojson_for_dt(self.testUUID, start_ld, end_ld)
        self.persistGroundTruthIfNeeded(api_result, dataFile, start_ld, cacheKey)

        # Although we process the day's data in two batches, we should get the same result
        self.compare_approx_result(ad.AttrDict({'result': api_result}).result,
                                   ad.AttrDict(ground_truth).data, time_fuzz=60, distance_fuzz=100)

    def testZeroDurationPlaceInterpolationSingleSync(self):
        # Test for 545114feb5ac15caac4110d39935612525954b71
        dataFile_1 = "emission/tests/data/real_examples/shankari_2016-01-12"
        dataFile_2 = "emission/tests/data/real_examples/shankari_2016-01-13"
        start_ld_1 = ecwl.LocalDate({'year': 2016, 'month': 1, 'day': 12})
        start_ld_2 = ecwl.LocalDate({'year': 2016, 'month': 1, 'day': 13})
        cacheKey_1 = "diary/trips-2016-01-12"
        cacheKey_2 = "diary/trips-2016-01-13"
        with open(dataFile_1+".ground_truth") as gtf1:
            ground_truth_1 = json.load(gtf1, object_hook=bju.object_hook)
        with open(dataFile_2+".ground_truth") as gtf2:
            ground_truth_2 = json.load(gtf2, object_hook=bju.object_hook)

        etc.setupRealExample(self, dataFile_1)
        with open(dataFile_2) as df2:
            self.entries = json.load(df2, object_hook = bju.object_hook)
        etc.setupRealExampleWithEntries(self)
        etc.runIntakePipeline(self.testUUID)

        api_result = gfc.get_geojson_for_dt(self.testUUID, start_ld_1, start_ld_1)
        # Although we process the day's data in two batches, we should get the same result
        self.persistGroundTruthIfNeeded(api_result, dataFile_1, start_ld_1, cacheKey_1)

        self.compare_result(ad.AttrDict({'result': api_result}).result,
                            ad.AttrDict(ground_truth_1).data)

        api_result = gfc.get_geojson_for_dt(self.testUUID, start_ld_2, start_ld_2)
        # Although we process the day's data in two batches, we should get the same result
        self.persistGroundTruthIfNeeded(api_result, dataFile_2, start_ld_2, cacheKey_2)
        self.compare_result(ad.AttrDict({'result': api_result}).result,
                            ad.AttrDict(ground_truth_2).data)

    def testZeroDurationPlaceInterpolationMultiSync(self):
        # Test for 545114feb5ac15caac4110d39935612525954b71
        dataFile_1 = "emission/tests/data/real_examples/shankari_2016-01-12"
        dataFile_2 = "emission/tests/data/real_examples/shankari_2016-01-13"
        start_ld_1 = ecwl.LocalDate({'year': 2016, 'month': 1, 'day': 12})
        start_ld_2 = ecwl.LocalDate({'year': 2016, 'month': 1, 'day': 13})
        cacheKey_1 = "diary/trips-2016-01-12"
        cacheKey_2 = "diary/trips-2016-01-13"
        with open(dataFile_1+".ground_truth") as gtf1:
            ground_truth_1 = json.load(gtf1, object_hook=bju.object_hook)
        with open(dataFile_2+".ground_truth") as gtf2:
            ground_truth_2 = json.load(gtf2, object_hook=bju.object_hook)

        etc.setupRealExample(self, dataFile_1)
        etc.runIntakePipeline(self.testUUID)
        with open(dataFile_2) as df2:
            self.entries = json.load(df2, object_hook = bju.object_hook)
        etc.setupRealExampleWithEntries(self)
        etc.runIntakePipeline(self.testUUID)

        api_result = gfc.get_geojson_for_dt(self.testUUID, start_ld_1, start_ld_1)
        # Although we process the day's data in two batches, we should get the same result
        self.persistGroundTruthIfNeeded(api_result, dataFile_1, start_ld_1, cacheKey_1)
        self.compare_result(ad.AttrDict({'result': api_result}).result,
                            ad.AttrDict(ground_truth_1).data)

        api_result = gfc.get_geojson_for_dt(self.testUUID, start_ld_2, start_ld_2)
        # Although we process the day's data in two batches, we should get the same result
        self.persistGroundTruthIfNeeded(api_result, dataFile_2, start_ld_2, cacheKey_2)
        self.compare_result(ad.AttrDict({'result': api_result}).result,
                            ad.AttrDict(ground_truth_2).data)

    def testTsMismatch(self):
        # Test for https://github.com/e-mission/e-mission-server/issues/457
        dataFile = "emission/tests/data/real_examples/shankari_single_positional_indexer.dec-12"
        start_ld = ecwl.LocalDate({'year': 2016, 'month': 12, 'day': 12})
        cacheKey = "diary/trips-2016-12-12"
        self.standardMatchDataGroundTruth(dataFile, start_ld, cacheKey)

    def testOverriddenModeHack(self):
        # Test for https://github.com/e-mission/e-mission-server/issues/457
        dataFile = "emission/tests/data/real_examples/test_overriden_mode_hack.jul-31"
        start_ld = ecwl.LocalDate({'year': 2017, 'month': 7, 'day': 31})
        cacheKey = "diary/trips-2017-07-31"
        self.standardMatchDataGroundTruth(dataFile, start_ld, cacheKey)

    def testJan16SpeedAssert(self):
        # Test for https://github.com/e-mission/e-mission-server/issues/457
        dataFile = "emission/tests/data/real_examples/another_speed_assertion_failure.jan-16"
        start_ld = ecwl.LocalDate({'year': 2016, 'month': 1, 'day': 16})
        cacheKey = "diary/trips-2016-01-16"
        self.standardMatchDataGroundTruth(dataFile, start_ld, cacheKey)

if __name__ == '__main__':
    etc.configLogging()

    parser = argparse.ArgumentParser()
    parser.add_argument("--algo_change",
        help="modifications to the algorithm", action="store_true")
    unittest.main()
