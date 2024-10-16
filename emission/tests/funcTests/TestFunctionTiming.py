# emission/tests/funcTests/TestFunctionTiming.py

import unittest
import logging
import time
import typing as t
import pymongo

# Import the store_dashboard_time and store_dashboard_error functions
import emission.storage.decorations.stats_queries as sdq

# Import the existing Timer context manager
import emission.core.timer as ec_timer

# Import the database module for verification
import emission.storage.timeseries.abstract_timeseries as esta
import emission.core.get_database as edb

class TestFunctionTiming(unittest.TestCase):
    @classmethod
    def setUpClass(self):
        """
        Set up resources before any tests are run.
        """
        logging.basicConfig(level=logging.INFO)
        self.timeseries_db = esta.TimeSeries.get_time_series(None)

    def tearDown(self):
        """
        Clean up relevant database entries after each test to maintain isolation.
        """
        # Define the metadata keys to clean
        keys_to_clean = ["stats/dashboard_time", "stats/dashboard_error"]
        try:
            # Delete entries where metadata.key is in keys_to_clean from timeseries_db
            edb.get_timeseries_db().delete_many({"metadata.key": {"$in": keys_to_clean}})
            logging.info("Cleaned up database entries after test based on metadata keys.")
        except Exception as e:
            logging.error(f"Error cleaning up database entries: {e}")
            raise


    def test_function_no_delay(self):
        """
        Test execution and timing of test_function_1.
        """
        self.execute_and_time_function(test_function_no_delay)

    def test_function_short_delay(self):
        """
        Test execution and timing of test_function_2.
        """
        self.execute_and_time_function(test_function_short_delay)
    
    def test_function_long_delay(self):
        """
        Test execution and timing of test_function_3.
        """
        self.execute_and_time_function(test_function_long_delay)

    def test_function_faulty(self):
        """
        Test execution and timing of test_function_faulty, which is expected to raise an exception.
        """
        with self.assertRaises(ValueError) as context:
            self.execute_and_time_function(test_function_faulty)
        self.assertIn("Simulated error in test_function_faulty", str(context.exception))

    def execute_and_time_function(self, func: t.Callable[[], bool]):
        """
        Executes a given function, measures its execution time using ECT_Timer,
        stores the timing information using store_dashboard_time, and verifies
        that the data was stored successfully by querying the timeseries database.
        If the function raises an exception, it stores the error using store_dashboard_error
        and verifies the error storage.

        Parameters:
        - func (Callable[[], bool]): The test function to execute and time.
        """
        function_name = func.__name__
        logging.info(f"Starting timing for function: {function_name}")

        try:
            with ec_timer.Timer() as timer:
                result = func()  # Execute the test function
                
            elapsed_ms = (timer.elapsed * 1000)  # Convert to milliseconds

            # Store the execution time
            sdq.store_dashboard_time(
                code_fragment_name=function_name,
                timer=timer
            )
            logging.info(f"Function '{function_name}' executed successfully in {elapsed_ms} ms.")

            # Verification: Adjusted Query to Match Document Structure
            stored_documents_chain = self.timeseries_db.find_entries(["stats/dashboard_time"], time_query=None)

            # Convert the chain to a list to make it subscriptable and to allow multiple accesses
            stored_document = list(stored_documents_chain)

            # Assert that at least one document was retrieved
            self.assertTrue(
                len(stored_document) > 0,
                f"Data for '{function_name}' was not found in the database."
            )
            stored_document = stored_document[0]
            # Iterate over each document and inspect its contents
            try:
                stored_ts = stored_document['data']['ts']
                stored_reading = stored_document['data']['reading']
                stored_name = stored_document['data']['name']
                logging.debug(
                    f"Stored Document for '{function_name}': ts={stored_ts}, reading={stored_reading}, name={stored_name}"
                )
            except KeyError as e:
                self.fail(
                    f"Missing key {e} in stored document for '{function_name}'."
                )
            
            # Assert that the stored_reading_error matches elapsed_ms exactly
            self.assertEqual(
                stored_reading,
                elapsed_ms,
                msg=(
                    f"Stored reading {stored_reading} ms does not exactly match "
                    f"elapsed time {elapsed_ms:.2f} ms for '{function_name}'."
                )
            )

            # Assert that the stored_name_error matches function_name exactly
            self.assertEqual(
                stored_name,
                function_name,
                msg=(
                    f"Stored name '{stored_name}' does not match function "
                    f"name '{function_name}'."
                )
            )

        except Exception as e:
            # Even if the function fails, capture the elapsed time up to the exception
            elapsed_seconds = timer.elapsed if 'timer' in locals() else 0  # Accessing the float attribute directly
            elapsed_ms = (elapsed_seconds * 1000)  # Convert to milliseconds

            # Store the error timing
            sdq.store_dashboard_error(
                code_fragment_name=function_name,
                timer=timer
            )
            logging.error(f"Function '{function_name}' failed after {elapsed_ms} ms with error: {e}")

            # Verification: Adjusted Error Query to Match Document Structure
            stored_error_chain = self.timeseries_db.find_entries(["stats/dashboard_error"], time_query=None)

            # Convert the chain to a list to make it subscriptable and to allow multiple accesses
            stored_error = list(stored_error_chain)

            # Assert that at least one document was retrieved
            self.assertTrue(
                len(stored_error) > 0,
                f"Data for '{function_name}' was not found in the database."
            )
            stored_error = stored_error[0]
            # Iterate over each document and inspect its contents
            try:
                stored_ts_error = stored_error['data']['ts']
                stored_reading_error = stored_error['data']['reading']
                stored_name_error = stored_error['data']['name']
                logging.debug(
                    f"Stored Document for '{function_name}': ts={stored_ts_error}, reading={stored_reading_error}, name={stored_name_error}"
                )
            except KeyError as e:
                self.fail(
                    f"Missing key {e} in stored document for '{function_name}'."
                )
            # Assert that the stored_reading_error matches elapsed_ms exactly
            self.assertEqual(
                stored_reading_error,
                elapsed_ms,
                msg=(
                    f"Stored error reading {stored_reading_error} ms does not exactly match "
                    f"elapsed time {elapsed_ms:.2f} ms for '{function_name}'."
                )
            )

            # Assert that the stored_name_error matches function_name exactly
            self.assertEqual(
                stored_name_error,
                function_name,
                msg=(
                    f"Stored error name '{stored_name_error}' does not match function "
                    f"name '{function_name}'."
                )
            )

            # Re-raise the exception to let the test fail
            raise


# Define the test functions outside the TestCase class
def test_function_no_delay():
    logging.info("Executing test_function_no_delay")
    return True

def test_function_short_delay():
    logging.info("Executing test_function_short_delay")
    time.sleep(1)  # Simulate processing time
    return True  # Indicate successful execution

def test_function_long_delay():
    logging.info("Executing test_function_long_delay")
    time.sleep(5)
    return True

def test_function_faulty():
    logging.info("Executing test_function_faulty")
    time.sleep(1)
    raise ValueError("Simulated error in test_function_faulty")


if __name__ == "__main__":
    unittest.main()
