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
import pymongo
import json
import emission.storage.json_wrappers as esj
import pandas as pd
from uuid import UUID
import os

# Our imports
import emission.core.get_database as edb
import emission.core.wrapper.pipelinestate as ecwp

import emission.analysis.intake.cleaning.filter_accuracy as eaicf
import emission.storage.timeseries.abstract_timeseries as esta
import emission.storage.pipeline_queries as epq

import emission.tests.common as etc

class TestFilterAccuracy(unittest.TestCase):
    def setUp(self):
        # We need to access the database directly sometimes in order to
        # forcibly insert entries for the tests to pass. But we put the import
        # in here to reduce the temptation to use the database directly elsewhere.
        import emission.core.get_database as edb
        import uuid

        self.analysis_conf_path = \
            etc.set_analysis_config("intake.cleaning.filter_accuracy.enable", True)
        self.testUUID = None

    def tearDown(self):
        import emission.core.get_database as edb
        edb.get_timeseries_db().delete_many({"user_id": self.testUUID})
        edb.get_pipeline_state_db().delete_many({"user_id": self.testUUID})
        os.remove(self.analysis_conf_path)

    def checkSuccessfulRun(self):
        pipelineState = edb.get_pipeline_state_db().find_one({"user_id": self.testUUID,
            "pipeline_stage": ecwp.PipelineStages.ACCURACY_FILTERING.value})
        self.assertIsNotNone(pipelineState["last_ts_run"])
        
    def testEmptyCallToPriorDuplicate(self):
        dataFile = "emission/tests/data/smoothing_data/tablet_2015-11-03"
        etc.setupRealExample(self, dataFile)
        self.ts = esta.TimeSeries.get_time_series(self.testUUID)
        time_query = epq.get_time_range_for_accuracy_filtering(self.testUUID)
        unfiltered_points_df = self.ts.get_data_df("background/location", time_query)
        self.assertEqual(len(unfiltered_points_df), 205)

        # Check call to check duplicate with a zero length dataframe
        entry = unfiltered_points_df.iloc[5]
        self.assertEqual(eaicf.check_prior_duplicate(pd.DataFrame(), 0, entry), False)

    def testEmptyCall(self):
        dataFile = "emission/tests/data/smoothing_data/tablet_2015-11-03"
        etc.setupRealExample(self, dataFile)
        self.ts = esta.TimeSeries.get_time_series(self.testUUID)
        # Check call to the entire filter accuracy with a zero length timeseries
        import emission.core.get_database as edb
        edb.get_timeseries_db().delete_many({"user_id": self.testUUID})
        # We expect that this should not throw
        eaicf.filter_accuracy(self.testUUID)
        self.assertEqual(len(self.ts.get_data_df("background/location")), 0)
        self.checkSuccessfulRun()

    def testCheckPriorDuplicate(self):
        dataFile = "emission/tests/data/smoothing_data/tablet_2015-11-03"
        etc.setupRealExample(self, dataFile)
        self.ts = esta.TimeSeries.get_time_series(self.testUUID)
        time_query = epq.get_time_range_for_accuracy_filtering(self.testUUID)
        unfiltered_points_df = self.ts.get_data_df("background/location", time_query)
        self.assertEqual(len(unfiltered_points_df), 205)

        entry = unfiltered_points_df.iloc[5]
        unfiltered_appended_df = pd.DataFrame([entry] * 5).append(unfiltered_points_df).reset_index()
        logging.debug("unfiltered_appended_df = %s" % unfiltered_appended_df[["fmt_time"]].head())

        self.assertEqual(eaicf.check_prior_duplicate(unfiltered_appended_df, 0, entry), False)
        self.assertEqual(eaicf.check_prior_duplicate(unfiltered_appended_df, 5, entry), True)
        self.assertEqual(eaicf.check_prior_duplicate(unfiltered_points_df, 5, entry), False)
        
    def testConvertToFiltered(self):
        dataFile = "emission/tests/data/smoothing_data/tablet_2015-11-03"
        etc.setupRealExample(self, dataFile)
        self.ts = esta.TimeSeries.get_time_series(self.testUUID)
        time_query = epq.get_time_range_for_accuracy_filtering(self.testUUID)
        unfiltered_points_df = self.ts.get_data_df("background/location", time_query)
        self.assertEqual(len(unfiltered_points_df), 205)

        entry_from_df = unfiltered_points_df.iloc[5]
        entry_copy = eaicf.convert_to_filtered(self.ts.get_entry_at_ts("background/location",
                                        "metadata.write_ts",
                                        entry_from_df.metadata_write_ts))
        self.assertNotIn("_id", entry_copy)
        self.assertEqual(entry_copy["metadata"]["key"], "background/filtered_location")

    def testExistingFilteredLocation(self):
        dataFile = "emission/tests/data/smoothing_data/tablet_2015-11-03"
        etc.setupRealExample(self, dataFile)
        self.ts = esta.TimeSeries.get_time_series(self.testUUID)
        time_query = epq.get_time_range_for_accuracy_filtering(self.testUUID)
        unfiltered_points_df = self.ts.get_data_df("background/location", time_query)
        self.assertEqual(len(unfiltered_points_df), 205)

        entry_from_df = unfiltered_points_df.iloc[5]
        logging.debug("entry_from_df: data.ts = %s, metadata.ts = %s" % 
            (entry_from_df.ts, entry_from_df.metadata_write_ts))
        self.assertEqual(eaicf.check_existing_filtered_location(self.ts, entry_from_df), False)

        entry_copy = self.ts.get_entry_at_ts("background/location", "metadata.write_ts",
                                            entry_from_df.metadata_write_ts)
        self.ts.insert(eaicf.convert_to_filtered(entry_copy))
        self.assertEqual(eaicf.check_existing_filtered_location(self.ts, entry_from_df), True)

    def testFilterAccuracy(self):
        dataFile = "emission/tests/data/smoothing_data/tablet_2015-11-03"
        etc.setupRealExample(self, dataFile)
        self.ts = esta.TimeSeries.get_time_series(self.testUUID)
        unfiltered_points_df = self.ts.get_data_df("background/location", None)
        self.assertEqual(len(unfiltered_points_df), 205)
        pre_filtered_points_df = self.ts.get_data_df("background/filtered_location", None)
        self.assertEqual(len(pre_filtered_points_df), 0)

        eaicf.filter_accuracy(self.testUUID)
        filtered_points_df = self.ts.get_data_df("background/filtered_location", None)
        self.assertEqual(len(filtered_points_df), 124)
        self.checkSuccessfulRun()

    def testFilterAccuracyWithPartialFiltered(self):
        dataFile = "emission/tests/data/real_examples/shankari_2016-independence_day"
        etc.setupRealExample(self, dataFile)
        self.ts = esta.TimeSeries.get_time_series(self.testUUID)
        unfiltered_points_df = self.ts.get_data_df("background/location", None)
        self.assertEqual(len(unfiltered_points_df), 801)
        pre_filtered_points_df = self.ts.get_data_df("background/filtered_location", None)
        self.assertEqual(len(pre_filtered_points_df), 703)

        cutoff_ts = pre_filtered_points_df.iloc[200].ts
        del_result = edb.get_timeseries_db().delete_many({
                "user_id": self.testUUID,
                "metadata.key": "background/filtered_location",
                "data.ts": {"$gte": cutoff_ts}
            })
        self.assertEqual(del_result.raw_result["n"], 503)

        post_cutoff_points_df = self.ts.get_data_df("background/filtered_location", None)
        self.assertEqual(len(post_cutoff_points_df), 200)

        eaicf.filter_accuracy(self.testUUID)
        filtered_points_df = self.ts.get_data_df("background/filtered_location", None)
        self.assertEqual(len(filtered_points_df), 703)
        self.checkSuccessfulRun()

    def testPandasMergeBehavior(self):
        import pandas as pd
        df_a = pd.DataFrame({"ts": [1,2,3,4]})
        df_b = pd.DataFrame({"ts": [1,3]})
        merged_left_idx = df_a.merge(df_b, on="ts", how="inner")
        merged_right_idx = df_a.reset_index().merge(df_b, on="ts", how="inner")
        merged_right_idx.set_index('index', inplace=True)
        self.assertEqual(merged_left_idx.index.to_list(), [0,1])
        self.assertEqual(merged_right_idx.index.to_list(), [0,2])

if __name__ == '__main__':
    etc.configLogging()
    unittest.main()
