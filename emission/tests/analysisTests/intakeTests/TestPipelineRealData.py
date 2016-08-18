import unittest
import logging
import json
import bson.json_util as bju
import attrdict as ad

# Our imports
import emission.core.get_database as edb
import emission.core.wrapper.localdate as ecwl

import emission.net.usercache.abstract_usercache_handler as enuah
import emission.analysis.plotting.geojson.geojson_feature_converter as gfc
import emission.storage.timeseries.tcquery as estt

# Test imports
import emission.tests.common as etc

class TestPipelineRealData(unittest.TestCase):
    def setUp(self):
        pass

    def tearDown(self):
        self.clearRelatedDb()

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
            logging.debug(rt.properties)
            logging.debug(et.properties)
            # Highly user visible
            self.assertEqual(rt.properties.start_ts, et.properties.start_ts)
            self.assertEqual(rt.properties.end_ts, et.properties.end_ts)
            self.assertEqual(rt.properties.start_loc, et.properties.start_loc)
            self.assertEqual(rt.properties.end_loc, et.properties.end_loc)
            self.assertAlmostEqual(rt.properties.distance, et.properties.distance, places=2)
            self.assertEqual(len(rt.features), len(et.features))

            for rs, es in zip(rt.features, et.features):
                logging.debug("------- Comparing trip feature ---------")
                logging.debug(rs)
                logging.debug(es)
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

    def testJun21(self):
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

if __name__ == '__main__':
    etc.configLogging()
    unittest.main()
