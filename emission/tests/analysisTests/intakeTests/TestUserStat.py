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

        #etc.runIntakePipeline(self.testUUID)
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

    def testGetAndStoreUserStatsDefault(self):
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

    def testGetAndStoreUserStatsSecondRunNoNewData(self):
        """
        Case (ii): Verify stats remain unchanged if we run the pipeline again
        without adding new data.
        """
        # Check stats after the initial run (from setUp()).
        initial_profile = edb.get_profile_db().find_one({"user_id": self.testUUID})
        self.assertIsNotNone(initial_profile, "User profile should exist after first run.")
        initial_total_trips = initial_profile["total_trips"]
        initial_labeled_trips = initial_profile["labeled_trips"]

        # Run the pipeline again, but don't add any new data
        etc.runIntakePipeline(self.testUUID)

        # Stats should remain the same
        updated_profile = edb.get_profile_db().find_one({"user_id": self.testUUID})
        self.assertIsNotNone(updated_profile, "Profile should still exist.")
        self.assertEqual(
            updated_profile["total_trips"], 
            initial_total_trips,
            f"Expected total_trips to remain {initial_total_trips}, got {updated_profile['total_trips']}"
        )
        self.assertEqual(
            updated_profile["labeled_trips"], 
            initial_labeled_trips,
            f"Expected labeled_trips to remain {initial_labeled_trips}, got {updated_profile['labeled_trips']}"
        )


    def testGetAndStoreUserStatsNewData(self):
        """
        Case (i): Verify stats are updated properly when new data is inserted
        from shankari_2015-aug-27 without modifying the original data and the pipeline is rerun.
        We then assert the actual number of total trips (e.g., from 8 to 18).
        """
        # 1. Retrieve the initial user profile after setUp()
        initial_profile = edb.get_profile_db().find_one({"user_id": self.testUUID})
        self.assertIsNotNone(initial_profile, "User profile should exist after the first run.")

        # 2. Assert that the initial total trips are as expected (8 trips)
        expected_initial_trips = 8
        self.assertEqual(
            initial_profile["total_trips"],
            expected_initial_trips,
            f"Expected initial total_trips to be {expected_initial_trips}, got {initial_profile['total_trips']}"
        )

        # Store initial trips count and labeled trips for later comparison
        initial_total_trips = initial_profile["total_trips"]
        initial_labeled_trips = initial_profile["labeled_trips"]

        # 3. Load and prepare new data from shankari_2015-aug-27
        new_entries = []
        aug27_file_path = "emission/tests/data/real_examples/shankari_2015-aug-27"

        try:
            with open(aug27_file_path) as fp:
                # Load entries using the existing JSON wrapper
                aug27_entries = json.load(fp, object_hook=esj.wrapped_object_hook)
                for entry in aug27_entries:
                    # Replace the user_id UUID with self.testUUID
                    entry['user_id'] = self.testUUID

                    # Remove the '_id' field to let MongoDB assign a new one
                    if '_id' in entry:
                        del entry['_id']

                    # Append the modified entry to the new_entries list
                    new_entries.append(entry)
                    
        except FileNotFoundError:
            self.fail(f"New data file not found at path: {aug27_file_path}")
        except json.JSONDecodeError as e:
            self.fail(f"JSON decoding failed for file {aug27_file_path}: {e}")

        # 4. Insert the new entries into the timeseries collection
        if new_entries:
            edb.get_timeseries_db().insert_many(new_entries)
        else:
            self.fail("No new entries were loaded from the new data file.")

        # 5. Run the pipeline again to process the newly inserted entries
        etc.runIntakePipeline(self.testUUID)

        # 6. Retrieve the updated user profile after processing new data
        updated_profile = edb.get_profile_db().find_one({"user_id": self.testUUID})
        self.assertIsNotNone(updated_profile, "Profile should exist after inserting new data.")

        # 7. Assert that the total trips have increased from 8 to 18
        expected_final_trips = 18
        self.assertEqual(
            updated_profile["total_trips"],
            expected_final_trips,
            f"Expected total_trips to be {expected_final_trips}, got {updated_profile['total_trips']}"
        )

        # 8. Ensure that labeled_trips is not less than it was before
        self.assertGreaterEqual(
            updated_profile["labeled_trips"],
            initial_labeled_trips,
            f"Expected labeled_trips >= {initial_labeled_trips}, got {updated_profile['labeled_trips']}"
        )


if __name__ == '__main__':
    # Configure logging for the test
    etc.configLogging()
    unittest.main()
