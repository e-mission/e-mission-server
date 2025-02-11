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
import emission.storage.timeseries.abstract_timeseries as esta
import emission.core.wrapper.user as ecwu
import emission.analysis.result.user_stat as user_stats

# Test imports
import emission.tests.common as etc
import emission.net.api.stats as enac

class TestUserStats(unittest.TestCase):
    def setUp(self):
        """
        Set up the test environment by loading real example data.
        """
        self.testUUID = uuid.uuid4()
        # Load example entries (this file must exist and contain valid test data)
        with open("emission/tests/data/real_examples/shankari_2015-aug-21") as fp:
            self.entries = json.load(fp, object_hook=esj.wrapped_object_hook)
        etc.setupRealExampleWithEntries(self)
        # Ensure that a profile exists for this user.
        profile = edb.get_profile_db().find_one({"user_id": self.testUUID})
        if profile is None:
            edb.get_profile_db().insert_one({"user_id": self.testUUID})
        
        # Run the intake pipeline via the function from 'etc'
        etc.runIntakePipeline(self.testUUID)
        logging.debug("UUID = %s", self.testUUID)

    def tearDown(self):
        """
        Clean up the test environment by removing test data.
        """
        edb.get_timeseries_db().delete_many({"user_id": self.testUUID})
        edb.get_pipeline_state_db().delete_many({"user_id": self.testUUID})
        edb.get_analysis_timeseries_db().delete_many({"user_id": self.testUUID})
        edb.get_profile_db().delete_one({"user_id": self.testUUID})

    def testPipelineDependentStats(self):
        """
        Test that pipeline-dependent user stats (e.g., total trips, labeled trips,
        and pipeline range) are correctly aggregated and stored.
        """
        profile = edb.get_profile_db().find_one({"user_id": self.testUUID})
        self.assertIsNotNone(profile, "User profile should exist after storing stats.")

        # Verify that the profile contains the keys updated by the pipeline-dependent functions.
        self.assertIn("total_trips", profile, "User profile should contain 'total_trips'.")
        self.assertIn("labeled_trips", profile, "User profile should contain 'labeled_trips'.")
        self.assertIn("pipeline_range", profile, "User profile should contain 'pipeline_range'.")

        expected_total_trips = 5
        expected_labeled_trips = 0

        self.assertEqual(
            profile["total_trips"],
            expected_total_trips,
            f"Expected total_trips to be {expected_total_trips}, got {profile['total_trips']}"
        )
        self.assertEqual(
            profile["labeled_trips"],
            expected_labeled_trips,
            f"Expected labeled_trips to be {expected_labeled_trips}, got {profile['labeled_trips']}"
        )

        pipeline_range = profile.get("pipeline_range", {})
        self.assertIn("start_ts", pipeline_range, "Pipeline range should contain 'start_ts'.")
        self.assertIn("end_ts", pipeline_range, "Pipeline range should contain 'end_ts'.")

        # These expected timestamps should match the test data...
        expected_start_ts = 1440168891.095
        expected_end_ts = 1440209488.817

        self.assertEqual(
            pipeline_range["start_ts"],
            expected_start_ts,
            f"Expected start_ts to be {expected_start_ts}, got {pipeline_range['start_ts']}"
        )
        self.assertEqual(
            pipeline_range["end_ts"],
            expected_end_ts,
            f"Expected end_ts to be {expected_end_ts}, got {pipeline_range['end_ts']}"
        )

    def testPipelineIndependentStats(self):
        """
        Test that the pipeline-independent user stat (last_call_ts) is correctly updated.
        """
        # Store a new API call timestamp.
        test_call_ts = time.time()
        enac.store_server_api_time(self.testUUID, "test_call_ts", test_call_ts, 69420)
        
        # Update only the pipeline-independent stats.
        user_stats.get_and_store_pipeline_independent_user_stats(self.testUUID)
        
        profile = edb.get_profile_db().find_one({"user_id": self.testUUID})
        expected_last_call_ts = test_call_ts
        actual_last_call_ts = profile.get("last_call_ts")

        self.assertEqual(
            actual_last_call_ts,
            expected_last_call_ts,
            f"Expected last_call_ts to be {expected_last_call_ts}, got {actual_last_call_ts}"
        )

if __name__ == '__main__':
    etc.configLogging()
    unittest.main()
