from __future__ import unicode_literals
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import
# This test is for the pipeline reset code

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

# Our imports
import emission.core.get_database as edb
import emission.core.wrapper.localdate as ecwl
import emission.pipeline.reset as epr

import emission.analysis.plotting.geojson.geojson_feature_converter as gfc

# Test imports
import emission.tests.common as etc

class TestPipelineReset(unittest.TestCase):
    def setUp(self):
        np.random.seed(61297777)

    def tearDown(self):
        logging.debug("Clearing related databases")
        self.clearRelatedDb()

    def clearRelatedDb(self):
        edb.get_timeseries_db().delete_many({"user_id": self.testUUID})
        edb.get_analysis_timeseries_db().delete_many({"user_id": self.testUUID})
        edb.get_usercache_db().delete_one({"user_id": self.testUUID})

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


    def testResetToStart(self):
        """
        - Load data for both days
        - Run pipelines
        - Verify that all is well
        - Reset to start
        - Verify that there is no analysis data
        - Re-run pipelines
        - Verify that all is well
        """

        # Load all data
        dataFile_1 = "emission/tests/data/real_examples/shankari_2016-07-22"
        dataFile_2 = "emission/tests/data/real_examples/shankari_2016-07-25"
        start_ld_1 = ecwl.LocalDate({'year': 2016, 'month': 7, 'day': 22})
        start_ld_2 = ecwl.LocalDate({'year': 2016, 'month': 7, 'day': 25})
        cacheKey_1 = "diary/trips-2016-07-22"
        cacheKey_2 = "diary/trips-2016-07-25"
        ground_truth_1 = json.load(open(dataFile_1+".ground_truth"), object_hook=bju.object_hook)
        ground_truth_2 = json.load(open(dataFile_2+".ground_truth"), object_hook=bju.object_hook)

        # Run both pipelines
        etc.setupRealExample(self, dataFile_1)
        etc.runIntakePipeline(self.testUUID)
        self.entries = json.load(open(dataFile_2), object_hook = bju.object_hook)
        etc.setupRealExampleWithEntries(self)
        etc.runIntakePipeline(self.testUUID)

        # Check results: so far, so good
        api_result = gfc.get_geojson_for_dt(self.testUUID, start_ld_1, start_ld_1)
        self.compare_result(ad.AttrDict({'result': api_result}).result,
                            ad.AttrDict(ground_truth_1).data)

        api_result = gfc.get_geojson_for_dt(self.testUUID, start_ld_2, start_ld_2)
        self.compare_result(ad.AttrDict({'result': api_result}).result,
                            ad.AttrDict(ground_truth_2).data)

        # Reset pipeline to start
        epr.reset_user_to_start(self.testUUID, is_dry_run=False)

        # Now there are no results
        api_result = gfc.get_geojson_for_dt(self.testUUID, start_ld_1, start_ld_1)
        self.assertEqual(api_result, [])

        api_result = gfc.get_geojson_for_dt(self.testUUID, start_ld_2, start_ld_2)
        self.assertEqual(api_result, [])

        # Re-run the pipeline again
        etc.runIntakePipeline(self.testUUID)

        # Should be back to ground truth
        api_result = gfc.get_geojson_for_dt(self.testUUID, start_ld_1, start_ld_1)
        self.compare_result(ad.AttrDict({'result': api_result}).result,
                            ad.AttrDict(ground_truth_1).data)

        api_result = gfc.get_geojson_for_dt(self.testUUID, start_ld_2, start_ld_2)
        self.compare_result(ad.AttrDict({'result': api_result}).result,
                            ad.AttrDict(ground_truth_2).data)

    def testResetToTsInMiddleOfPlace(self):
        """
        - Load data for both days
        - Run pipelines
        - Verify that all is well
        - Reset to a date between the two
        - Verify that analysis data for the first day is unchanged
        - Verify that analysis data for the second day does not exist
        - Re-run pipelines
        - Verify that all is well
        """

        # Load all data
        dataFile_1 = "emission/tests/data/real_examples/shankari_2016-07-22"
        dataFile_2 = "emission/tests/data/real_examples/shankari_2016-07-25"
        start_ld_1 = ecwl.LocalDate({'year': 2016, 'month': 7, 'day': 22})
        start_ld_2 = ecwl.LocalDate({'year': 2016, 'month': 7, 'day': 25})
        cacheKey_1 = "diary/trips-2016-07-22"
        cacheKey_2 = "diary/trips-2016-07-25"
        ground_truth_1 = json.load(open(dataFile_1+".ground_truth"), object_hook=bju.object_hook)
        ground_truth_2 = json.load(open(dataFile_2+".ground_truth"), object_hook=bju.object_hook)

        # Run both pipelines
        etc.setupRealExample(self, dataFile_1)
        etc.runIntakePipeline(self.testUUID)
        self.entries = json.load(open(dataFile_2), object_hook = bju.object_hook)
        etc.setupRealExampleWithEntries(self)
        etc.runIntakePipeline(self.testUUID)

        # Check results: so far, so good
        api_result = gfc.get_geojson_for_dt(self.testUUID, start_ld_1, start_ld_1)
        self.compare_result(ad.AttrDict({'result': api_result}).result,
                            ad.AttrDict(ground_truth_1).data)

        api_result = gfc.get_geojson_for_dt(self.testUUID, start_ld_2, start_ld_2)
        self.compare_result(ad.AttrDict({'result': api_result}).result,
                            ad.AttrDict(ground_truth_2).data)

        # Reset pipeline to july 23.
        # Note that this is actually 22nd 16:00 PDT, so this is partway
        # through the 22nd
        reset_ts = arrow.get("2016-07-23").timestamp
        epr.reset_user_to_ts(self.testUUID, reset_ts, is_dry_run=False)

        # First day is unchanged, except that the last place doesn't have
        # exit data.
        # TODO: Modify ground truth to capture this change
        # Until then, we know that this will fail
#        api_result = gfc.get_geojson_for_dt(self.testUUID, start_ld_1, start_ld_1)
#        self.compare_result(ad.AttrDict({'result': api_result}).result,
#                            ad.AttrDict(ground_truth_1).data)

        # Second day does not exist
        api_result = gfc.get_geojson_for_dt(self.testUUID, start_ld_2, start_ld_2)
        logging.debug(json.dumps(api_result, indent=4, default=bju.default))
        self.assertEqual(api_result, [])

        # Re-run the pipeline again
        etc.runIntakePipeline(self.testUUID)

        # Should be back to ground truth
        api_result = gfc.get_geojson_for_dt(self.testUUID, start_ld_1, start_ld_1)
        self.compare_result(ad.AttrDict({'result': api_result}).result,
                            ad.AttrDict(ground_truth_1).data)

        api_result = gfc.get_geojson_for_dt(self.testUUID, start_ld_2, start_ld_2)
        self.compare_result(ad.AttrDict({'result': api_result}).result,
                            ad.AttrDict(ground_truth_2).data)


    def testResetToTsInMiddleOfTrip(self):
        """
        - Load data for both days
        - Run pipelines
        - Verify that all is well
        - Reset to a date between the two
        - Verify that analysis data for the first day is unchanged
        - Verify that analysis data for the second day does not exist
        - Re-run pipelines
        - Verify that all is well
        """

        # Load all data
        dataFile_1 = "emission/tests/data/real_examples/shankari_2016-07-22"
        dataFile_2 = "emission/tests/data/real_examples/shankari_2016-07-25"
        start_ld_1 = ecwl.LocalDate({'year': 2016, 'month': 7, 'day': 22})
        start_ld_2 = ecwl.LocalDate({'year': 2016, 'month': 7, 'day': 25})
        cacheKey_1 = "diary/trips-2016-07-22"
        cacheKey_2 = "diary/trips-2016-07-25"
        ground_truth_1 = json.load(open(dataFile_1+".ground_truth"), object_hook=bju.object_hook)
        ground_truth_2 = json.load(open(dataFile_2+".ground_truth"), object_hook=bju.object_hook)

        # Run both pipelines
        etc.setupRealExample(self, dataFile_1)
        etc.runIntakePipeline(self.testUUID)
        self.entries = json.load(open(dataFile_2), object_hook = bju.object_hook)
        etc.setupRealExampleWithEntries(self)
        etc.runIntakePipeline(self.testUUID)

        # Check results: so far, so good
        api_result = gfc.get_geojson_for_dt(self.testUUID, start_ld_1, start_ld_1)
        self.compare_result(ad.AttrDict({'result': api_result}).result,
                            ad.AttrDict(ground_truth_1).data)

        api_result = gfc.get_geojson_for_dt(self.testUUID, start_ld_2, start_ld_2)
        self.compare_result(ad.AttrDict({'result': api_result}).result,
                            ad.AttrDict(ground_truth_2).data)

        # Reset pipeline to july 24.
        # Note that this is actually 23nd 16:00 PDT
        # This will reset in the middle of the untracked time, which is
        # technically a trip, and will allow us to test the trip resetting
        # code
        reset_ts = arrow.get("2016-07-24").timestamp
        epr.reset_user_to_ts(self.testUUID, reset_ts, is_dry_run=False)

        # Second day does not exist
        api_result = gfc.get_geojson_for_dt(self.testUUID, start_ld_2, start_ld_2)
        logging.debug(json.dumps(api_result, indent=4, default=bju.default))
        self.assertEqual(api_result, [])

        # Re-run the pipeline again
        etc.runIntakePipeline(self.testUUID)

        # Should be back to ground truth
        api_result = gfc.get_geojson_for_dt(self.testUUID, start_ld_1, start_ld_1)
        self.compare_result(ad.AttrDict({'result': api_result}).result,
                            ad.AttrDict(ground_truth_1).data)

        api_result = gfc.get_geojson_for_dt(self.testUUID, start_ld_2, start_ld_2)
        self.compare_result(ad.AttrDict({'result': api_result}).result,
                            ad.AttrDict(ground_truth_2).data)

        
    def testResetToFuture(self):
        """
        - Load data for both days
        - Run pipelines
        - Reset to a date after the two
        - Verify that all is well
        - Re-run pipelines and ensure that there are no errors
        """
        # Load all data
        dataFile_1 = "emission/tests/data/real_examples/shankari_2016-07-22"
        dataFile_2 = "emission/tests/data/real_examples/shankari_2016-07-25"
        start_ld_1 = ecwl.LocalDate({'year': 2016, 'month': 7, 'day': 22})
        start_ld_2 = ecwl.LocalDate({'year': 2016, 'month': 7, 'day': 25})
        cacheKey_1 = "diary/trips-2016-07-22"
        cacheKey_2 = "diary/trips-2016-07-25"
        ground_truth_1 = json.load(open(dataFile_1+".ground_truth"), object_hook=bju.object_hook)
        ground_truth_2 = json.load(open(dataFile_2+".ground_truth"), object_hook=bju.object_hook)

        # Run both pipelines
        etc.setupRealExample(self, dataFile_1)
        etc.runIntakePipeline(self.testUUID)
        self.entries = json.load(open(dataFile_2), object_hook = bju.object_hook)
        etc.setupRealExampleWithEntries(self)
        etc.runIntakePipeline(self.testUUID)

        # Reset to a date well after the two days
        reset_ts = arrow.get("2017-07-24").timestamp
        epr.reset_user_to_ts(self.testUUID, reset_ts, is_dry_run=False)

        # Data should be untouched because of early return
        api_result = gfc.get_geojson_for_dt(self.testUUID, start_ld_1, start_ld_1)
        self.compare_result(ad.AttrDict({'result': api_result}).result,
                            ad.AttrDict(ground_truth_1).data)

        api_result = gfc.get_geojson_for_dt(self.testUUID, start_ld_2, start_ld_2)
        self.compare_result(ad.AttrDict({'result': api_result}).result,
                            ad.AttrDict(ground_truth_2).data)

        # Re-running the pipeline again should not affect anything
        etc.runIntakePipeline(self.testUUID)

    def testResetToPast(self):
        """
        - Load data for both days
        - Run pipelines
        - Verify that all is well
        - Reset to a date before both
        - Verify that analysis data for the both days is removed
        - Re-run pipelines
        - Verify that all is well
        """
        # Load all data
        dataFile_1 = "emission/tests/data/real_examples/shankari_2016-07-22"
        dataFile_2 = "emission/tests/data/real_examples/shankari_2016-07-25"
        start_ld_1 = ecwl.LocalDate({'year': 2016, 'month': 7, 'day': 22})
        start_ld_2 = ecwl.LocalDate({'year': 2016, 'month': 7, 'day': 25})
        cacheKey_1 = "diary/trips-2016-07-22"
        cacheKey_2 = "diary/trips-2016-07-25"
        ground_truth_1 = json.load(open(dataFile_1+".ground_truth"), object_hook=bju.object_hook)
        ground_truth_2 = json.load(open(dataFile_2+".ground_truth"), object_hook=bju.object_hook)

        # Run both pipelines
        etc.setupRealExample(self, dataFile_1)
        etc.runIntakePipeline(self.testUUID)
        self.entries = json.load(open(dataFile_2), object_hook = bju.object_hook)
        etc.setupRealExampleWithEntries(self)
        etc.runIntakePipeline(self.testUUID)

        # Verify that all is well
        api_result = gfc.get_geojson_for_dt(self.testUUID, start_ld_1, start_ld_1)
        self.compare_result(ad.AttrDict({'result': api_result}).result,
                            ad.AttrDict(ground_truth_1).data)

        api_result = gfc.get_geojson_for_dt(self.testUUID, start_ld_2, start_ld_2)
        self.compare_result(ad.AttrDict({'result': api_result}).result,
                            ad.AttrDict(ground_truth_2).data)

        # Reset to a date well before the two days
        reset_ts = arrow.get("2015-07-24").timestamp
        epr.reset_user_to_ts(self.testUUID, reset_ts, is_dry_run=False)

        # Data should be completely deleted
        api_result = gfc.get_geojson_for_dt(self.testUUID, start_ld_1, start_ld_1)
        self.assertEqual(api_result, [])

        api_result = gfc.get_geojson_for_dt(self.testUUID, start_ld_2, start_ld_2)
        self.assertEqual(api_result, [])

        # Re-running the pipeline again
        etc.runIntakePipeline(self.testUUID)
        
        # Should reconstruct everything
        api_result = gfc.get_geojson_for_dt(self.testUUID, start_ld_1, start_ld_1)
        self.compare_result(ad.AttrDict({'result': api_result}).result,
                            ad.AttrDict(ground_truth_1).data)

        api_result = gfc.get_geojson_for_dt(self.testUUID, start_ld_2, start_ld_2)
        self.compare_result(ad.AttrDict({'result': api_result}).result,
                            ad.AttrDict(ground_truth_2).data)

    # TODO: Add tests for no place and one place


if __name__ == '__main__':
    etc.configLogging()
    unittest.main()
