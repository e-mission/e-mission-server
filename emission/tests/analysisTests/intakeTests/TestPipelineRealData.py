# This test compares the output of the intake pipeline
# with known ground truth output.
# The way to add a new test is:
# - load the test timeline
# $ ./e-mission-py.bash bin/debug/load_timeline_for_day_and_user.py emission/tests/data/real_examples/iphone_2016-02-22
# - run the intake pipeline
# $ ./e-mission-py.bash bin/intake_stage.py
# - log in via the phone and check that all is well
# - generate ground truth
# >>> tj = edb.get_usercache_db().find_one({'metadata.key': 'diary/trips-2016-02-22'})
# >>> import attrdict as ad
# >>> import json
# >>> import bson.json_util as bju
# >>> json.dump(tj, open("/tmp/iphone_2016-02-22.ground_truth", "w"), indent=4, default=bju.default)
# - move the ground truth back
# $ mv /tmp/iphone_2016-02-22.ground_truth emission/tests/data/real_examples

import unittest
import logging
import json
import bson.json_util as bju
import attrdict as ad
import arrow

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
        pass

    def tearDown(self):
        logging.debug("Clearing related databases")
        # self.clearRelatedDb()

    def clearRelatedDb(self):
        edb.get_timeseries_db().remove({"user_id": self.testUUID})
        edb.get_analysis_timeseries_db().remove({"user_id": self.testUUID})
        edb.get_usercache_db().remove({"user_id": self.testUUID})

    def compare_result(self, result, expect):
        # This is basically a bunch of asserts to ensure that the timeline is as
        # expected. We are not using a recursive diff because things like the IDs
        # will change from run to run. Instead, I pick out a bunch of important
        # things that are highly user visible
        # Since this is deterministic, we can also include things that are not that user visible :)

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

    def testJun20(self):
        # This is a fairly straightforward day. Tests mainly:
        # - ordering of trips
        # - handling repeated location entries with different write timestamps
        # We have two identical location points with ts = 1466436483.395 and write_ts = 1466436496.4, 1466436497.047
        dataFile = "emission/tests/data/real_examples/shankari_2016-06-20"
        ld = ecwl.LocalDate({'year': 2016, 'month': 6, 'day': 20})
        cacheKey = "diary/trips-2016-06-20"
        ground_truth = json.load(open(dataFile+".ground_truth"), object_hook=bju.object_hook)

        etc.setupRealExample(self, dataFile)
        etc.runIntakePipeline(self.testUUID)
        # runIntakePipeline does not run the common trips, habitica or store views to cache
        # So let's manually store to the cache
        # tc_query = estt.TimeComponentQuery("data.star_local_dt", ld, ld)
        # enuah.UserCacheHandler.getUserCacheHandler(self.testUUID).storeTimelineToCache(tc_query)

        # cached_result = edb.get_usercache_db().find_one({'user_id': self.testUUID,
        #                                                  "metadata.key": cacheKey})
        api_result = gfc.get_geojson_for_dt(self.testUUID, ld, ld)

        # self.compare_result(cached_result, ground_truth)
        self.compare_result(ad.AttrDict({'result': api_result}).result,
                            ad.AttrDict(ground_truth).data)

    def testJun21(self):
        # This is a more complex day. Tests:
        # PR #357 (spurious trip at 14:00 should be segmented and skipped)
        # PR #358 (trip back from bella's house at 16:00)

        dataFile = "emission/tests/data/real_examples/shankari_2016-06-21"
        ld = ecwl.LocalDate({'year': 2016, 'month': 6, 'day': 21})
        cacheKey = "diary/trips-2016-06-21"
        ground_truth = json.load(open(dataFile+".ground_truth"), object_hook=bju.object_hook)

        etc.setupRealExample(self, dataFile)
        etc.runIntakePipeline(self.testUUID)
        # runIntakePipeline does not run the common trips, habitica or store views to cache
        # So let's manually store to the cache
        # tc_query = estt.TimeComponentQuery("data.star_local_dt", ld, ld)
        # enuah.UserCacheHandler.getUserCacheHandler(self.testUUID).storeTimelineToCache(tc_query)

        # cached_result = edb.get_usercache_db().find_one({'user_id': self.testUUID,
        #                                                  "metadata.key": cacheKey})
        api_result = gfc.get_geojson_for_dt(self.testUUID, ld, ld)

        # self.compare_result(cached_result, ground_truth)
        self.compare_result(ad.AttrDict({'result': api_result}).result,
                            ad.AttrDict(ground_truth).data)

    def testAug10(self):
        # This is a more complex day. Tests:
        # PR #302 (trip to optometrist)
        # PR #352 (split optometrist trip)

        dataFile = "emission/tests/data/real_examples/shankari_2016-08-10"
        ld = ecwl.LocalDate({'year': 2016, 'month': 8, 'day': 10})
        cacheKey = "diary/trips-2016-08-10"
        ground_truth = json.load(open(dataFile+".ground_truth"), object_hook=bju.object_hook)

        etc.setupRealExample(self, dataFile)
        etc.runIntakePipeline(self.testUUID)
        # runIntakePipeline does not run the common trips, habitica or store views to cache
        # So let's manually store to the cache
        # tc_query = estt.TimeComponentQuery("data.star_local_dt", ld, ld)
        # enuah.UserCacheHandler.getUserCacheHandler(self.testUUID).storeTimelineToCache(tc_query)

        # cached_result = edb.get_usercache_db().find_one({'user_id': self.testUUID,
        #                                                  "metadata.key": cacheKey})
        api_result = gfc.get_geojson_for_dt(self.testUUID, ld, ld)

        # self.compare_result(cached_result, ground_truth)
        self.compare_result(ad.AttrDict({'result': api_result}).result,
                            ad.AttrDict(ground_truth).data)

    def testAug11(self):
        # This is a more complex day. Tests:
        # PR #352 (should not split trip to Oakland)
        # PR #348 (trip from station to OAK DOT)
        # PR #357 (trip to Radio Shack is complete and not truncated)
        # PR #345 (no cleaned trips are skipped)

        dataFile = "emission/tests/data/real_examples/shankari_2016-08-11"
        ld = ecwl.LocalDate({'year': 2016, 'month': 8, 'day': 11})
        cacheKey = "diary/trips-2016-08-11"
        ground_truth = json.load(open(dataFile+".ground_truth"), object_hook=bju.object_hook)

        etc.setupRealExample(self, dataFile)
        etc.runIntakePipeline(self.testUUID)
        # runIntakePipeline does not run the common trips, habitica or store views to cache
        # So let's manually store to the cache
        # tc_query = estt.TimeComponentQuery("data.star_local_dt", ld, ld)
        # enuah.UserCacheHandler.getUserCacheHandler(self.testUUID).storeTimelineToCache(tc_query)

        # cached_result = edb.get_usercache_db().find_one({'user_id': self.testUUID,
        #                                                  "metadata.key": cacheKey})
        api_result = gfc.get_geojson_for_dt(self.testUUID, ld, ld)

        # self.compare_result(cached_result, ground_truth)
        self.compare_result(ad.AttrDict({'result': api_result}).result,
                            ad.AttrDict(ground_truth).data)

    def testFeb22ShortTripsDistance(self):
        # Re-run, but with multiple calls to sync data
        # This tests the effect of online versus offline analysis and segmentation with potentially partial data

        dataFile = "emission/tests/data/real_examples/iphone_3_2016-02-22"
        start_ld = ecwl.LocalDate({'year': 2016, 'month': 2, 'day': 22})
        end_ld = ecwl.LocalDate({'year': 2016, 'month': 2, 'day': 22})
        cacheKey = "diary/trips-2016-02-22"
        ground_truth = json.load(open(dataFile+".ground_truth"), object_hook=bju.object_hook)

        etc.setupRealExample(self, dataFile)
        etc.runIntakePipeline(self.testUUID)
        api_result = gfc.get_geojson_for_dt(self.testUUID, start_ld, end_ld)

        # Although we process the day's data in two batches, we should get the same result
        self.compare_result(ad.AttrDict({'result': api_result}).result,
                                   ad.AttrDict(ground_truth).data)

    def testAug10MultiSyncEndDetected(self):
        # Re-run, but with multiple calls to sync data
        # This tests the effect of online versus offline analysis and segmentation with potentially partial data

        dataFile = "emission/tests/data/real_examples/shankari_2016-08-10"
        start_ld = ecwl.LocalDate({'year': 2016, 'month': 8, 'day': 9})
        end_ld = ecwl.LocalDate({'year': 2016, 'month': 8, 'day': 10})
        cacheKey = "diary/trips-2016-08-10"
        ground_truth = json.load(open("emission/tests/data/real_examples/shankari_2016-08-910.ground_truth"),
                                 object_hook=bju.object_hook)

        logging.info("Before loading, timeseries db size = %s" % edb.get_timeseries_db().count())
        all_entries = json.load(open(dataFile), object_hook = bju.object_hook)
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
        ground_truth = json.load(open(dataFile+".ground_truth"), object_hook=bju.object_hook)

        logging.info("Before loading, timeseries db size = %s" % edb.get_timeseries_db().count())
        all_entries = json.load(open(dataFile), object_hook = bju.object_hook)
        # 18:01 because the transition was at 2016-02-22T18:00:09.623404-08:00, so right after
        # 18:00
        ts_1800 = arrow.get("2016-02-22T18:00:30-08:00").timestamp
        logging.debug("ts_1800 = %s, converted back = %s" % (ts_1800, arrow.get(ts_1800).to("America/Los_Angeles")))
        before_1800_entries = [e for e in all_entries if ad.AttrDict(e).metadata.write_ts <= ts_1800]
        after_1800_entries = [e for e in all_entries if ad.AttrDict(e).metadata.write_ts > ts_1800]

        # Sync at 18:00 to capture all the points on the trip *to* the optometrist
        import uuid
        self.testUUID = uuid.uuid4()
        self.entries = before_1800_entries
        etc.setupRealExampleWithEntries(self)
        etc.runIntakePipeline(self.testUUID)
        api_result = gfc.get_geojson_for_dt(self.testUUID, start_ld, end_ld)

        # Then sync after 18:00
        self.entries = after_1800_entries
        etc.setupRealExampleWithEntries(self)
        etc.runIntakePipeline(self.testUUID)
        api_result = gfc.get_geojson_for_dt(self.testUUID, start_ld, end_ld)

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
        ground_truth = json.load(open("emission/tests/data/real_examples/shankari_2016-08-910.ground_truth"),
                                 object_hook=bju.object_hook)

        logging.info("Before loading, timeseries db size = %s" % edb.get_timeseries_db().count())
        all_entries = json.load(open(dataFile), object_hook = bju.object_hook)
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

        # Although we process the day's data in two batches, we should get the same result
        self.compare_approx_result(ad.AttrDict({'result': api_result}).result,
                                   ad.AttrDict(ground_truth).data, time_fuzz=60, distance_fuzz=100)

    def testFeb22MultiSyncEndNotDetected(self):
        # Re-run, but with multiple calls to sync data
        # This tests the effect of online versus offline analysis and segmentation with potentially partial data

        dataFile = "emission/tests/data/real_examples/iphone_2016-02-22"
        start_ld = ecwl.LocalDate({'year': 2016, 'month': 2, 'day': 22})
        end_ld = ecwl.LocalDate({'year': 2016, 'month': 2, 'day': 22})
        cacheKey = "diary/trips-2016-02-22"
        ground_truth = json.load(open(dataFile+".ground_truth"), object_hook=bju.object_hook)

        logging.info("Before loading, timeseries db size = %s" % edb.get_timeseries_db().count())
        all_entries = json.load(open(dataFile), object_hook = bju.object_hook)
        # 18:01 because the transition was at 2016-02-22T18:00:09.623404-08:00, so right after
        # 18:00
        ts_1800 = arrow.get("2016-02-22T18:00:30-08:00").timestamp
        logging.debug("ts_1800 = %s, converted back = %s" % (ts_1800, arrow.get(ts_1800).to("America/Los_Angeles")))
        before_1800_entries = [e for e in all_entries if ad.AttrDict(e).metadata.write_ts <= ts_1800]
        after_1800_entries = [e for e in all_entries if ad.AttrDict(e).metadata.write_ts > ts_1800]

        # Sync at 18:00 to capture all the points on the trip *to* the optometrist
        # Skip the last few points to ensure that the trip end is skipped
        import uuid
        self.testUUID = uuid.uuid4()
        self.entries = before_1800_entries[0:-2]
        etc.setupRealExampleWithEntries(self)
        etc.runIntakePipeline(self.testUUID)
        api_result = gfc.get_geojson_for_dt(self.testUUID, start_ld, end_ld)

        # Then sync after 18:00
        self.entries = after_1800_entries
        etc.setupRealExampleWithEntries(self)
        etc.runIntakePipeline(self.testUUID)
        api_result = gfc.get_geojson_for_dt(self.testUUID, start_ld, end_ld)

        # Although we process the day's data in two batches, we should get the same result
        self.compare_approx_result(ad.AttrDict({'result': api_result}).result,
                                   ad.AttrDict(ground_truth).data, time_fuzz=60, distance_fuzz=100)

if __name__ == '__main__':
    etc.configLogging()
    unittest.main()
