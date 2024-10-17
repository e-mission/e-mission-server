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
        Verifies that each of these expected entries, in the form of (key, name, elapsed_ms),
        are stored in the database.

        :param expected_entries: A list of tuples which have (key, name, expected_elapsed_ms).
        """
        logging.debug(f"Ensuring {len(expected_entries)} entries exist in DB.")
        key_list = [key for (key, _, _) in expected_entries]
        stored_entrys = list(self.timeseries_db.find_entries(key_list))
        self.assertEqual(len(stored_entrys), len(expected_entries))

        for i in range(len(expected_entries)):
            stored_entry = stored_entrys[i]
            expected_key, expected_name, expected_reading = expected_entries[i]
            logging.debug(f"Comparing expected {expected_entries[i]} " +
                          f"with stored {stored_entry['metadata']['key']} {stored_entry['data']}")
            self.assertEqual(stored_entry['metadata']['key'], expected_key)
            self.assertEqual(stored_entry['data']['name'], expected_name)
            self.assertEqual(stored_entry['data']['reading'], expected_reading)

    def test_single_function_timing(self):
        """
        Test execution and timing of a single function.
        """
        def sample_function():
            logging.debug("Executing sample_function")
            time.sleep(2)  # Simulate processing time
            return True

        with ect.Timer() as timer:
            sample_function()

        esdsq.store_dashboard_time("sample_function", timer)

        # verify that an entry was stored correctly
        self.verify_stats_entries([
            ("stats/dashboard_time", "sample_function", timer.elapsed_ms)
        ])

    def test_multiple_functions_timing(self):
        """
        Test execution and timing of 2 functions, where we record and validate
        the time taken for: (i) function_one, (ii) function_two, and (iii) both functions together.
        """
        def function_one(): return time.sleep(1)
        def function_two(): return time.sleep(1.5)

        with ect.Timer() as timer_both:
            with ect.Timer() as timer_one:
                function_one()
            esdsq.store_dashboard_time("function_one", timer_one)

            with ect.Timer() as timer_two:
                function_two()
            esdsq.store_dashboard_time("function_two", timer_two)

        esdsq.store_dashboard_time("functions_one_and_two", timer_both)

        # function_one should've taken ~1000ms; function_two should've taken ~1500ms;
        # both functions together should've taken ~2500ms
        self.assertAlmostEqual(timer_one.elapsed_ms, 1000, delta=100)
        self.assertAlmostEqual(timer_two.elapsed_ms, 1500, delta=100)
        self.assertAlmostEqual(timer_both.elapsed_ms, 2500, delta=100)

        # verify that entries were stored correctly
        self.verify_stats_entries([
            ("stats/dashboard_time", "function_one", timer_one.elapsed_ms),
            ("stats/dashboard_time", "function_two", timer_two.elapsed_ms),
            ("stats/dashboard_time", "functions_one_and_two", timer_both.elapsed_ms)
        ])

    def test_faulty_function_timing(self):
        """
        Test execution and timing of a faulty function that is expected to
        raise an exception.
        """
        def faulty_function():
            logging.debug("Executing faulty_function")
            time.sleep(1)  # Simulate processing time before failure
            raise ValueError("Simulated error in faulty_function")

        try:
            with ect.Timer() as timer:
                faulty_function()
        except ValueError as e:
            logging.info(f"Caught expected error: {e}")
            esdsq.store_dashboard_error('faulty_function', timer)
            pass

        # verify that an entry was stored correctly
        self.verify_stats_entries([
            ("stats/dashboard_error", "faulty_function", timer.elapsed_ms)
        ])


if __name__ == '__main__':
    etc.configLogging()
    unittest.main()
