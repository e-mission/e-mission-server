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
        logging.debug(f"Ensuring {len(expected_entries)} entries exist in DB.")
        # Prepare keys for database query based on expected entries.
        key_list = [key for (key, _, _) in expected_entries]
        # Fetch matching entries from the timeseries database.
        stored_entries = self.timeseries_db.find_entries(key_list)
        # Check if the number of retrieved entries matches expectations.
        self.assertEqual(len(stored_entries), len(expected_entries))

        # Validate each stored entry against the expected data.
        for i in range(len(expected_entries)):
            stored_entry = stored_entries[i]
            expected_key, expected_name, expected_reading = expected_entries[i]
            logging.debug(f"Comparing expected {expected_entries[i]} " +
                          f"with stored {stored_entry['metadata']['key']} {stored_entry['data']}")
            # Verify the key matches.
            self.assertEqual(stored_entry['metadata']['key'], expected_key)
            # Verify the name matches.
            self.assertEqual(stored_entry['data']['name'], expected_name)
            # Verify the elapsed time is as expected.
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

        # Measure the execution time of 'sample_function'.
        with ect.Timer() as timer:
            sample_function()

        # Record the timing data in the database.
        esdsq.store_dashboard_time("sample_function", timer)

        # Confirm the timing was recorded correctly.
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

        # Track the total time for both functions.
        with ect.Timer() as timer_both:
            # Time 'function_one' execution.
            with ect.Timer() as timer_one:
                function_one()
            # Record 'function_one' timing.
            esdsq.store_dashboard_time("function_one", timer_one)

            # Time 'function_two' execution.
            with ect.Timer() as timer_two:
                function_two()
            # Record 'function_two' timing.
            esdsq.store_dashboard_time("function_two", timer_two)

        # Record the combined timing for both functions.
        esdsq.store_dashboard_time("functions_one_and_two", timer_both)

        # Validate individual and combined timings.
        self.assertAlmostEqual(timer_one.elapsed_ms, 1000, delta=100)
        self.assertAlmostEqual(timer_two.elapsed_ms, 1500, delta=100)
        self.assertAlmostEqual(timer_both.elapsed_ms, 2500, delta=100)

        # Ensure all timing entries are correctly stored.
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
            # Attempt to execute and time the faulty function.
            with ect.Timer() as timer:
                faulty_function()
        except ValueError as e:
            # Handle the expected exception and record the timing as an error.
            logging.info(f"Caught expected error: {e}")
            esdsq.store_dashboard_error('faulty_function', timer)
            # Continue after handling the exception.
            pass

        # Verify that the error timing was recorded.
        self.verify_stats_entries([
            ("stats/dashboard_error", "faulty_function", timer.elapsed_ms)
        ])



if __name__ == '__main__':
    etc.configLogging()
    unittest.main()
