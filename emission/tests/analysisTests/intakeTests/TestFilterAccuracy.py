# Standard imports
import unittest
import datetime as pydt
import logging
import pymongo
import json
import bson.json_util as bju
import pandas as pd

# Our imports
import emission.analysis.intake.cleaning.filter_accuracy as eaicf
import emission.storage.timeseries.abstract_timeseries as esta
import emission.storage.pipeline_queries as epq

class TestFilterAccuracy(unittest.TestCase):
    def setUp(self):
        # We need to access the database directly sometimes in order to
        # forcibly insert entries for the tests to pass. But we put the import
        # in here to reduce the temptation to use the database directly elsewhere.
        import emission.core.get_database as edb
        import uuid

        self.testUUID = uuid.uuid4()
        self.entries = json.load(open("emission/tests/data/smoothing_data/tablet_2015-11-03"),
                                 object_hook=bju.object_hook)
        for entry in self.entries:
            entry["user_id"] = self.testUUID
            edb.get_timeseries_db().save(entry)
        self.ts = esta.TimeSeries.get_time_series(self.testUUID)

    def tearDown(self):
        import emission.core.get_database as edb
        edb.get_timeseries_db().remove({"user_id": self.testUUID})
        edb.get_pipeline_state_db().remove({"user_id": self.testUUID})

    def testCheckPriorDuplicate(self):
        time_query = epq.get_time_range_for_accuracy_filtering(self.testUUID)
        unfiltered_points_df = self.ts.get_data_df("background/location", time_query)
        self.assertEqual(len(unfiltered_points_df), 205)

        entry = unfiltered_points_df.iloc[5]
        unfiltered_appended_df = pd.DataFrame([entry] * 5).append(unfiltered_points_df).reset_index()
        logging.debug("unfiltered_appended_df = %s" % unfiltered_appended_df[["fmt_time"]].head())

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
        self.assertEquals(entry_copy["metadata"]["key"], "background/filtered_location")

    def testExistingFilteredLocation(self):
        time_query = epq.get_time_range_for_accuracy_filtering(self.testUUID)
        unfiltered_points_df = self.ts.get_data_df("background/location", time_query)
        self.assertEqual(len(unfiltered_points_df), 205)

        entry_from_df = unfiltered_points_df.iloc[5]
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
    logging.basicConfig(level=logging.DEBUG)
    unittest.main()
