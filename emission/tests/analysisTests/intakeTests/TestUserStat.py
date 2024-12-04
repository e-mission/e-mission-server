from __future__ import unicode_literals, print_function, division, absolute_import
import unittest
import uuid
import logging
import json
import os
import time
import pandas as pd

from builtins import *
from future import standard_library
standard_library.install_aliases()

# Standard imports
import emission.storage.json_wrappers as esj

# Our imports
import emission.core.get_database as edb
import emission.storage.timeseries.timequery as estt
import emission.storage.timeseries.abstract_timeseries as esta
import emission.storage.decorations.analysis_timeseries_queries as esda
import emission.core.wrapper.user as ecwu
import emission.net.api.stats as enac

# Test imports
import emission.tests.common as etc


class TestUserStats(unittest.TestCase):
    def setUp(self):
        """
        Set up the test environment by loading real example data for both Android and  users.
        """
        # Configure logging for the test
        etc.configLogging()

        # Retrieve a test user UUID from the database
        get_example = pd.DataFrame(list(edb.get_uuid_db().find({}, {"user_email":1, "uuid": 1, "_id": 0})))
        if get_example.empty:
            self.fail("No users found in the database to perform tests.")
        test_user_id = get_example.iloc[0].uuid
        self.testUUID = test_user_id
        self.UUID = test_user_id
        # Load example entries from a JSON file
        with open("emission/tests/data/real_examples/shankari_2015-aug-27") as fp:
            self.entries = json.load(fp, object_hook=esj.wrapped_object_hook)

        # Set up the real example data with entries
        etc.setupRealExampleWithEntries(self)

        # Retrieve the user profile
        profile = edb.get_profile_db().find_one({"user_id": self.UUID})
        if profile is None:
            # Initialize the profile if it does not exist
            edb.get_profile_db().insert_one({"user_id": self.UUID})

        etc.runIntakePipeline(self.UUID)

        logging.debug("UUID = %s" % (self.UUID))

    def tearDown(self):
        """
        Clean up the test environment by removing analysis configuration and deleting test data from databases.
        """

        # Delete all time series entries for users
        tsdb = edb.get_timeseries_db()
        tsdb.delete_many({"user_id": self.UUID})

        # Delete all pipeline state entries for users
        pipeline_db = edb.get_pipeline_state_db()
        pipeline_db.delete_many({"user_id": self.UUID})

        # Delete all analysis time series entries for  users
        analysis_ts_db = edb.get_analysis_timeseries_db()
        analysis_ts_db.delete_many({"user_id": self.UUID})

        # Delete user profiles
        profile_db = edb.get_profile_db()
        profile_db.delete_one({"user_id": self.UUID})

    def testGetAndStoreUserStats(self):
        """
        Test get_and_store_user_stats for the user to ensure that user statistics
        are correctly aggregated and stored in the user profile.
        """

        # Retrieve the updated user profile from the database
        profile = edb.get_profile_db().find_one({"user_id": self.UUID})

        # Ensure that the profile exists
        self.assertIsNotNone(profile, "User profile should exist after storing stats.")

        # Verify that the expected fields are present
        self.assertIn("total_trips", profile, "User profile should contain 'total_trips'.")
        self.assertIn("labeled_trips", profile, "User profile should contain 'labeled_trips'.")
        self.assertIn("pipeline_range", profile, "User profile should contain 'pipeline_range'.")
        self.assertIn("last_call_ts", profile, "User profile should contain 'last_call_ts'.")

        expected_total_trips = 8
        expected_labeled_trips = 0

        self.assertEqual(profile["total_trips"], expected_total_trips,
                         f"Expected total_trips to be {expected_total_trips}, got {profile['total_trips']}")
        self.assertEqual(profile["labeled_trips"], expected_labeled_trips,
                         f"Expected labeled_trips to be {expected_labeled_trips}, got {profile['labeled_trips']}")

        # Verify pipeline range
        pipeline_range = profile.get("pipeline_range", {})
        self.assertIn("start_ts", pipeline_range, "Pipeline range should contain 'start_ts'.")
        self.assertIn("end_ts", pipeline_range, "Pipeline range should contain 'end_ts'.")

        expected_start_ts = 1440688739.672
        expected_end_ts = 1440729142.709

        self.assertEqual(pipeline_range["start_ts"], expected_start_ts,
                         f"Expected start_ts to be {expected_start_ts}, got {pipeline_range['start_ts']}")
        self.assertEqual(pipeline_range["end_ts"], expected_end_ts,
                         f"Expected end_ts to be {expected_end_ts}, got {pipeline_range['end_ts']}")

    def testLastCall(self):
        # Call the function with all required arguments
        enac.store_server_api_time(self.UUID, "test_call", 1440729142.709, 69)

        etc.runIntakePipeline(self.UUID)

        # Retrieve the profile from the database
        profile = edb.get_profile_db().find_one({"user_id": self.UUID})

        # Verify that last_call_ts is updated correctly
        expected_last_call_ts = 1440729142.709
        actual_last_call_ts = profile.get("last_call_ts")

        self.assertEqual(
            actual_last_call_ts,
            expected_last_call_ts,
            f"Expected last_call_ts to be {expected_last_call_ts}, got {actual_last_call_ts}"
        )

if __name__ == '__main__':
    unittest.main()
