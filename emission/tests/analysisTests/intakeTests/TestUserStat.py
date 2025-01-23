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
import emission.pipeline.intake_stage as epi

# Test imports
import emission.tests.common as etc


class TestUserStats(unittest.TestCase):
    def setUp(self):
        """
        Set up the test environment by loading real example data for both Android and  users.
        """
        # Set up the real example data with entries
        self.testUUID = uuid.uuid4()
        with open("emission/tests/data/real_examples/shankari_2015-aug-21") as fp:
            self.entries = json.load(fp, object_hook = esj.wrapped_object_hook)
        # Retrieve the user profile
        etc.setupRealExampleWithEntries(self)
        profile = edb.get_profile_db().find_one({"user_id": self.testUUID})
        if profile is None:
            # Initialize the profile if it does not exist
            edb.get_profile_db().insert_one({"user_id": self.testUUID})

        etc.runIntakePipeline(self.testUUID)
        logging.debug("UUID = %s" % (self.testUUID))

    def tearDown(self):
        """
        Clean up the test environment by removing analysis configuration and deleting test data from databases.
        """

        edb.get_timeseries_db().delete_many({"user_id": self.testUUID})
        edb.get_pipeline_state_db().delete_many({"user_id": self.testUUID})
        edb.get_analysis_timeseries_db().delete_many({"user_id": self.testUUID})
        edb.get_profile_db().delete_one({"user_id": self.testUUID})

    def testGetAndStoreUserStats(self):
        """
        Test get_and_store_user_stats for the user to ensure that user statistics
        are correctly aggregated and stored in the user profile.
        """

        # Retrieve the updated user profile from the database
        profile = edb.get_profile_db().find_one({"user_id": self.testUUID})

        # Ensure that the profile exists
        self.assertIsNotNone(profile, "User profile should exist after storing stats.")

        # Verify that the expected fields are present
        self.assertIn("total_trips", profile, "User profile should contain 'total_trips'.")
        self.assertIn("labeled_trips", profile, "User profile should contain 'labeled_trips'.")
        self.assertIn("pipeline_range", profile, "User profile should contain 'pipeline_range'.")
        self.assertIn("last_call_ts", profile, "User profile should contain 'last_call_ts'.")

        expected_total_trips = 5
        expected_labeled_trips = 0

        self.assertEqual(profile["total_trips"], expected_total_trips,
                         f"Expected total_trips to be {expected_total_trips}, got {profile['total_trips']}")
        self.assertEqual(profile["labeled_trips"], expected_labeled_trips,
                         f"Expected labeled_trips to be {expected_labeled_trips}, got {profile['labeled_trips']}")

        # Verify pipeline range
        pipeline_range = profile.get("pipeline_range", {})
        self.assertIn("start_ts", pipeline_range, "Pipeline range should contain 'start_ts'.")
        self.assertIn("end_ts", pipeline_range, "Pipeline range should contain 'end_ts'.")

        expected_start_ts = 1440168891.095
        expected_end_ts = 1440209488.817

        self.assertEqual(pipeline_range["start_ts"], expected_start_ts,
                         f"Expected start_ts to be {expected_start_ts}, got {pipeline_range['start_ts']}")
        self.assertEqual(pipeline_range["end_ts"], expected_end_ts,
                         f"Expected end_ts to be {expected_end_ts}, got {pipeline_range['end_ts']}")

    def testLastCall(self):
        # Call the function with all required arguments
        test_call_ts = time.time()
        enac.store_server_api_time(self.testUUID, "test_call_ts", test_call_ts, 69420)
        etc.runIntakePipeline(self.testUUID)

        # Retrieve the profile from the database
        profile = edb.get_profile_db().find_one({"user_id": self.testUUID})

        # Verify that last_call_ts is updated correctly
        expected_last_call_ts = test_call_ts
        actual_last_call_ts = profile.get("last_call_ts")

        self.assertEqual(
            actual_last_call_ts,
            expected_last_call_ts,
            f"Expected last_call_ts to be {expected_last_call_ts}, got {actual_last_call_ts}"
        )

if __name__ == '__main__':
    # Configure logging for the test
    etc.configLogging()
    unittest.main()
