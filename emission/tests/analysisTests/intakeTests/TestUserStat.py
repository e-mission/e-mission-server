from __future__ import unicode_literals, print_function, division, absolute_import
import unittest
import uuid
import logging
from datetime import datetime

import emission.analysis.result.user_stat as eaurs
import emission.storage.timeseries.abstract_timeseries as esta
import emission.core.wrapper.user as ecwu
import emission.core.get_database as edb
import emission.tests.common as etc

class TestGetAndStoreUserStats(unittest.TestCase):
    def setUp(self):
        """
        Register a test user and insert initial time series data.
        """
        etc.configLogging()
        self.test_email = f'testuser_{uuid.uuid4()}@example.com'
        self.user = ecwu.User.register(self.test_email)
        self.test_uuid = self.user.uuid
        
        # Ensure the user profile exists
        if not self.user.getProfile():
            ecwu.User.createProfile(self.test_uuid, datetime.now())
        
        ts = esta.TimeSeries.get_time_series(self.test_uuid)
        
        # Insert 10 'analysis/confirmed_trip' entries
        for i in range(10):
            trip_entry = {
                "metadata": {"key": "analysis/confirmed_trip", "write_ts": datetime.now().timestamp()},
                "data": {
                    "trip_id": str(uuid.uuid4()),
                    "user_input": {"input": "test"} if i < 7 else {}
                }
            }
            ts.insert(trip_entry)
        
        # Insert a single 'analysis/composite_trip' entry
        composite_trip = {
            "metadata": {"key": "analysis/composite_trip", "write_ts": datetime.now().timestamp()},
            "data": {"start_ts": 1609459200, "end_ts": 1612137600}
        }
        ts.insert(composite_trip)
        
        # Insert a 'stats/server_api_time' entry
        server_api_time = {
            "metadata": {"key": "stats/server_api_time", "write_ts": datetime.now().timestamp()},
            "data": {"ts": 1614556800}
        }
        ts.insert(server_api_time)
    
    def tearDown(self):
        """
        Unregister the test user to clean up the database.
        """
        try:
            ecwu.User.unregister(self.test_email)
        except Exception as e:
            logging.error(f"Failed to unregister user {self.test_email}: {e}")
    
    def test_correct_data(self):
        """
        Verify that statistics are correctly aggregated when all data is present.
        """
        eaurs.get_and_store_user_stats(self.test_uuid, "analysis/composite_trip")
        profile = self.user.getProfile()
        
        self.assertIn("pipeline_range", profile, "Missing 'pipeline_range' in profile.")
        self.assertEqual(profile["pipeline_range"]["start_ts"], 1609459200, "Incorrect 'start_ts'.")
        self.assertEqual(profile["pipeline_range"]["end_ts"], 1612137600, "Incorrect 'end_ts'.")
        
        self.assertEqual(profile.get("total_trips", None), 10, "'total_trips' should be 10.")
        self.assertEqual(profile.get("labeled_trips", None), 7, "'labeled_trips' should be 7.")
        
        self.assertEqual(profile.get("last_call_ts", None), 1614556800, "'last_call_ts' mismatch.")
    
    def test_no_trips(self):
        """
        Ensure that statistics are zeroed out when there are no trips.
        """
        tsdb = edb.get_timeseries_db()
        tsdb.delete_many({"user_id": self.test_uuid, "metadata.key": "analysis/confirmed_trip"})
        
        # Confirm deletion
        remaining_trips = tsdb.count_documents({"user_id": self.test_uuid, "metadata.key": "analysis/confirmed_trip"})
        self.assertEqual(remaining_trips, 0, "Confirmed trips were not deleted.")
        
        eaurs.get_and_store_user_stats(self.test_uuid, "analysis/composite_trip")
        profile = self.user.getProfile()
        
        self.assertEqual(profile.get("total_trips", None), 0, "'total_trips' should be 0.")
        self.assertEqual(profile.get("labeled_trips", None), 0, "'labeled_trips' should be 0.")
    
    def test_no_last_call(self):
        """
        Check that 'last_call_ts' is None when there is no server API time entry.
        """
        tsdb = edb.get_timeseries_db()
        tsdb.delete_many({"user_id": self.test_uuid, "metadata.key": "stats/server_api_time"})
        
        # Confirm deletion
        remaining_api_times = tsdb.count_documents({"user_id": self.test_uuid, "metadata.key": "stats/server_api_time"})
        self.assertEqual(remaining_api_times, 0, "Server API time entries were not deleted.")
        
        eaurs.get_and_store_user_stats(self.test_uuid, "analysis/composite_trip")
        profile = self.user.getProfile()
        
        self.assertIsNone(profile.get("last_call_ts", None), "'last_call_ts' should be None.")
    
    def test_partial_data(self):
        """
        Verify behavior when 'analysis/composite_trip' data is missing.
        """
        tsdb = edb.get_timeseries_db()
        tsdb.delete_many({"user_id": self.test_uuid, "metadata.key": "analysis/composite_trip"})
        
        # Confirm deletion
        remaining_composite_trips = tsdb.count_documents({"user_id": self.test_uuid, "metadata.key": "analysis/composite_trip"})
        self.assertEqual(remaining_composite_trips, 0, "Composite trips were not deleted.")
        
        eaurs.get_and_store_user_stats(self.test_uuid, "analysis/composite_trip")
        profile = self.user.getProfile()
        
        self.assertIsNone(profile["pipeline_range"].get("start_ts"), "'start_ts' should be None.")
        self.assertIsNone(profile["pipeline_range"].get("end_ts"), "'end_ts' should be None.")
        
        self.assertEqual(profile.get("total_trips", None), 10, "'total_trips' should remain 10.")
        self.assertEqual(profile.get("labeled_trips", None), 7, "'labeled_trips' should remain 7.")
    
    def test_multiple_calls(self):
        """
        Ensure that multiple invocations correctly update statistics without duplication.
        """
        # Initial call
        eaurs.get_and_store_user_stats(self.test_uuid, "analysis/composite_trip")
        
        ts = esta.TimeSeries.get_time_series(self.test_uuid)
        
        # Insert additional trips
        for i in range(5):
            trip_entry = {
                "metadata": {"key": "analysis/confirmed_trip", "write_ts": datetime.now().timestamp()},
                "data": {
                    "trip_id": str(uuid.uuid4()),
                    "user_input": {"input": "additional_test"} if i < 3 else {}
                }
            }
            ts.insert(trip_entry)
        
        # Insert new server API time entry
        new_server_api_time = {
            "metadata": {"key": "stats/server_api_time", "write_ts": datetime.now().timestamp()},
            "data": {"ts": 1617235200}
        }
        ts.insert(new_server_api_time)
        
        # Second call
        eaurs.get_and_store_user_stats(self.test_uuid, "analysis/composite_trip")
        profile = self.user.getProfile()
        
        self.assertEqual(profile.get("total_trips", None), 15, "'total_trips' should be 15 after additional inserts.")
        self.assertEqual(profile.get("labeled_trips", None), 10, "'labeled_trips' should be 10 after additional inserts.")
        self.assertEqual(profile.get("last_call_ts", None), 1617235200, "'last_call_ts' should be updated to 1617235200.")
    
    def test_exception_handling(self):
        """
        Test handling of invalid UUID inputs.
        """
        invalid_uuid = "invalid-uuid-string"
        try:
            eaurs.get_and_store_user_stats(invalid_uuid, "analysis/composite_trip")
        except Exception as e:
            self.fail(f"get_and_store_user_stats raised an exception with invalid UUID: {e}")
        else:
            logging.debug("Handled invalid UUID without raising exceptions.")

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    unittest.main()
