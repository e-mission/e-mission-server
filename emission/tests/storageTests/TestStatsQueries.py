import unittest
import logging
import time

import emission.core.get_database as edb
import emission.core.timer as ect
import emission.storage.decorations.stats_queries as esdsq
import emission.storage.timeseries.abstract_timeseries as esta
import emission.tests.common as etc


class TestFunctionTiming(unittest.TestCase):
    @classmethod
    def setUpClass(self):
        """
        Set up resources before any tests are run.
        """
        self.timeseries_db = esta.TimeSeries.get_time_series(None)

    def tearDown(self):
        """
        Clean up relevant database entries after each test to maintain isolation.
        """
        keys_to_clean = ["stats/dashboard_time", "stats/dashboard_error"]
        edb.get_timeseries_db().delete_many(
            {"metadata.key": {"$in": keys_to_clean}}
        )
        logging.debug(f"After test, cleared DB entries for {keys_to_clean}")

    def verify_stats_entries(self, expected_entries: list[tuple[str, str, float]]):
        """
        Verifies that each of the expected entries, in the form of (key, name, elapsed_ms),
        are stored correctly in the database.

        :param expected_entries: A list of tuples containing (key, name, expected_elapsed_ms).
        """
        # Log the number of entries expected to exist in the database.
        logging.debug(f"Ensuring {len(expected_entries)} entries exist in DB.")
        # Extract the keys from the expected entries for querying the database.
        key_list = [key for (key, _, _) in expected_entries]
        # Retrieve the stored entries from the database matching the keys.
        stored_entrys = list(self.timeseries_db.find_entries(key_list))
        # Assert that the number of stored entries matches the number of expected entries.
        self.assertEqual(len(stored_entrys), len(expected_entries))

        # Iterate over each expected entry to verify its correctness.
        for i in range(len(expected_entries)):
            stored_entry = stored_entrys[i]
            expected_key, expected_name, expected_reading = expected_entries[i]
            # Log the comparison between expected and stored entries.
            logging.debug(f"Comparing expected {expected_entries[i]} " +
                          f"with stored {stored_entry['metadata']['key']} {stored_entry['data']}")
            # Assert that the stored key matches the expected key.
            self.assertEqual(stored_entry['metadata']['key'], expected_key)
            # Assert that the stored name matches the expected name.
            self.assertEqual(stored_entry['data']['name'], expected_name)
            # Assert that the stored reading (elapsed time) matches the expected value.
            self.assertEqual(stored_entry['data']['reading'], expected_reading)

    def test_single_function_timing(self):
        """
        Test the execution and timing of a single function.
        This test measures how long 'sample_function' takes to execute and verifies
        that the timing information is correctly stored in the database.
        """
        def sample_function():
            logging.debug("Executing sample_function")
            time.sleep(2)  # Simulate processing time by sleeping for 2 seconds.
            return True

        # Use the Timer context manager to measure the execution time of 'sample_function'.
        with ect.Timer() as timer:
            sample_function()

        # Store the timing information in the database under the key 'sample_function'.
        esdsq.store_dashboard_time("sample_function", timer)

        # Verify that the timing entry was stored correctly in the database.
        self.verify_stats_entries([
            ("stats/dashboard_time", "sample_function", timer.elapsed_ms)
        ])

    def test_multiple_functions_timing(self):
        """
        Test the execution and timing of two functions.
        This test records and validates the time taken for:
        (i) function_one,
        (ii) function_two, and
        (iii) both functions together.
        """
        def function_one():
            # Simulate processing time by sleeping for 1 second.
            return time.sleep(1)

        def function_two():
            # Simulate processing time by sleeping for 1.5 seconds.
            return time.sleep(1.5)

        # Start the overall timer for both functions.
        with ect.Timer() as timer_both:
            # Start and stop the timer for 'function_one'.
            with ect.Timer() as timer_one:
                function_one()
            # Store the timing information for 'function_one'.
            esdsq.store_dashboard_time("function_one", timer_one)

            # Start and stop the timer for 'function_two'.
            with ect.Timer() as timer_two:
                function_two()
            # Store the timing information for 'function_two'.
            esdsq.store_dashboard_time("function_two", timer_two)

        # Store the combined timing information for both functions.
        esdsq.store_dashboard_time("functions_one_and_two", timer_both)

        # Assert that the elapsed time for 'function_one' is approximately 1000ms (1 second).
        self.assertAlmostEqual(timer_one.elapsed_ms, 1000, delta=100)
        # Assert that the elapsed time for 'function_two' is approximately 1500ms (1.5 seconds).
        self.assertAlmostEqual(timer_two.elapsed_ms, 1500, delta=100)
        # Assert that the combined elapsed time is approximately 2500ms (2.5 seconds).
        self.assertAlmostEqual(timer_both.elapsed_ms, 2500, delta=100)

        # Verify that all timing entries were stored correctly in the database.
        self.verify_stats_entries([
            ("stats/dashboard_time", "function_one", timer_one.elapsed_ms),
            ("stats/dashboard_time", "function_two", timer_two.elapsed_ms),
            ("stats/dashboard_time", "functions_one_and_two", timer_both.elapsed_ms)
        ])

    def test_faulty_function_timing(self):
        """
        Test the execution and timing of a faulty function that is expected to raise an exception.
        This test ensures that even when a function fails, the timing information is correctly
        recorded as an error in the database.
        """
        def faulty_function():
            logging.debug("Executing faulty_function")
            time.sleep(1)  # Simulate processing time before failure.
            raise ValueError("Simulated error in faulty_function")

        try:
            # Attempt to execute the faulty function while timing its execution.
            with ect.Timer() as timer:
                faulty_function()
        except ValueError as e:
            # Catch the expected ValueError exception.
            logging.info(f"Caught expected error: {e}")
            # Store the timing information as an error in the database under 'faulty_function'.
            esdsq.store_dashboard_error('faulty_function', timer)
            # Pass to continue execution after handling the exception.
            pass

        # Verify that the error timing entry was stored correctly in the database.
        self.verify_stats_entries([
            ("stats/dashboard_error", "faulty_function", timer.elapsed_ms)
        ])


if __name__ == '__main__':
    # Configure logging settings before running the tests.
    etc.configLogging()
    # Run all the test cases defined in the TestFunctionTiming class.
    unittest.main()
    