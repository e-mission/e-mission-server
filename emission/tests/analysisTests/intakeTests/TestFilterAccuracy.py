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
import bson.json_util as bju
import pandas as pd
from uuid import UUID
import os

# Our imports
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
        self.testUUID = UUID('079e0f1a-c440-3d7c-b0e7-de160f748e35')
        with open("emission/tests/data/smoothing_data/tablet_2015-11-03") as fp:
            self.entries = json.load(fp,
                                 object_hook=bju.object_hook)
        tsdb = edb.get_timeseries_db()
        for entry in self.entries:
            entry["user_id"] = self.testUUID
            tsdb.insert_one(entry)
        self.ts = esta.TimeSeries.get_time_series(self.testUUID)

    def tearDown(self):
        import emission.core.get_database as edb
        edb.get_timeseries_db().delete_many({"user_id": self.testUUID})
        edb.get_pipeline_state_db().delete_many({"user_id": self.testUUID})
        os.remove(self.analysis_conf_path)
        
    def testEmptyCallToPriorDuplicate(self):
        time_query = epq.get_time_range_for_accuracy_filtering(self.testUUID)
        unfiltered_points_df = self.ts.get_data_df("background/location", time_query)
        self.assertEqual(len(unfiltered_points_df), 205)

        # Check call to check duplicate with a zero length dataframe
        entry = unfiltered_points_df.iloc[5]
        self.assertEqual(eaicf.check_prior_duplicate(pd.DataFrame(), 0, entry), False)

    def testEmptyCall(self):
        # Check call to the entire filter accuracy with a zero length timeseries
        import emission.core.get_database as edb
        edb.get_timeseries_db().delete_many({"user_id": self.testUUID})
        # We expect that this should not throw
        eaicf.filter_accuracy(self.testUUID)
        self.assertEqual(len(self.ts.get_data_df("background/location")), 0)

    def testCheckPriorDuplicate(self):
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
        unfiltered_points_df = self.ts.get_data_df("background/location", None)
        self.assertEqual(len(unfiltered_points_df), 205)
        pre_filtered_points_df = self.ts.get_data_df("background/filtered_location", None)
        self.assertEqual(len(pre_filtered_points_df), 0)

        eaicf.filter_accuracy(self.testUUID)
        filtered_points_df = self.ts.get_data_df("background/filtered_location", None)
        self.assertEqual(len(filtered_points_df), 124)

if __name__ == '__main__':
    etc.configLogging()
    unittest.main()
