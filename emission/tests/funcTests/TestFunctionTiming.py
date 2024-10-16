# emission/tests/funcTests/TestFunctionTiming.py

import unittest
import logging
import time
import typing as t
from contextlib import contextmanager

# Import the store_dashboard_time and store_dashboard_error functions
import emission.storage.decorations.stats_queries as sdq

# Import the existing Timer context manager
import emission.core.timer as ec_timer

# Import the database module for verification
import emission.storage.timeseries.abstract_timeseries as esta
import emission.core.get_database as edb

# No need to copy pasting logging info for every test B)
@contextmanager
def test_logger(test_name: str):
    """
    Context manager for logging the start and end of a test.
    """
    logging.info(f"\n########### Starting {test_name} ###########")
    try:
        yield
    finally:
        logging.info(f"########### Finished {test_name} ###########\n")


class TestFunctionTiming(unittest.TestCase):
    @classmethod
    def setUpClass(self):
        """
        Set up resources before any tests are run.
        """
        logging.basicConfig(level=logging.INFO, format='%(message)s')
        self.timeseries_db = esta.TimeSeries.get_time_series(None)

    def tearDown(self):
        """
        Clean up relevant database entries after each test to maintain isolation.
        """
        keys_to_clean = ["stats/dashboard_time", "stats/dashboard_error"]
        try:
            edb.get_timeseries_db().delete_many({"metadata.key": {"$in": keys_to_clean}})
            logging.info("Cleaned up database entries after test based on metadata keys.")
        except Exception as e:
            logging.error(f"Error cleaning up database entries: {e}")
            raise

    def run_with_timing(
        self, 
        name: str, 
        func: t.Callable[[], bool],
        store_success: t.Callable[[str, ec_timer.Timer], None] = sdq.store_dashboard_time,
        store_error: t.Callable[[str, ec_timer.Timer], None] = sdq.store_dashboard_error
    ):
        """
        Executes a function with timing, stores the timing or error data, and verifies the storage.

        Parameters:
        - name (str): The name of the function or code block.
        - func (Callable[[], bool]): The function to execute and time.
        - store_success (Callable): Function to store timing data on success.
        - store_error (Callable): Function to store error data on failure.
        """
        logging.info(f"Starting timing for '{name}'.")

        # Mapping of storage functions and corresponding keys
        storage_mapping = {
            'success': (store_success, "stats/dashboard_time"),
            'error': (store_error, "stats/dashboard_error")
        }

        try:
            with ec_timer.Timer() as timer:
                result = func()  # Execute the function

            elapsed_ms = timer.elapsed * 1000  # Convert to milliseconds

            # Store the execution time using the appropriate storage function
            store_func, key = storage_mapping['success']
            store_func(code_fragment_name=name, timer=timer)
            logging.info(f"'{name}' executed successfully in {elapsed_ms:.2f} ms.")

            # Verify the timing entry
            self.verify_entry(key, name, elapsed_ms)

        except Exception as e:
            # Capture elapsed time up to the exception
            elapsed_seconds = timer.elapsed if 'timer' in locals() else 0
            elapsed_ms = elapsed_seconds * 1000  # Convert to milliseconds

            # Store the error timing using the appropriate storage function
            store_func, key = storage_mapping['error']
            store_func(code_fragment_name=name, timer=timer)
            logging.error(f"'{name}' failed after {elapsed_ms:.2f} ms with error: {e}")

            # Verify the error entry
            self.verify_entry(key, name, elapsed_ms)

            # Re-raise the exception to let the test fail
            raise

    def verify_entry(self, key: str, name: str, expected_elapsed_ms: float):
        """
        Verifies that an entry for the given name exists under the specified key and matches the expected elapsed time.

        Parameters:
        - key (str): The database key to query ("stats/dashboard_time" or "stats/dashboard_error").
        - name (str): The name of the function or code block.
        - expected_elapsed_ms (float): The expected elapsed time in milliseconds.
        """
        logging.info(f"Verifying entry for '{name}' under key '{key}'.")
        stored_documents_chain = self.timeseries_db.find_entries(
            [key],
            extra_query_list=[{"data.name": name}]
        )
        stored_documents = list(stored_documents_chain)

        self.assertEqual(
            len(stored_documents),
            1,
            f"Expected exactly 1 entry for '{name}' under key '{key}', found {len(stored_documents)}."
        )
        stored_document = stored_documents[0]

        try:
            stored_reading = stored_document['data']['reading']
            stored_name = stored_document['data']['name']
            logging.info(
                f"Stored Document for '{name}': reading={stored_reading} ms, name={stored_name}"
            )
        except KeyError as e:
            self.fail(
                f"Missing key {e} in stored document for '{name}'."
            )

        # Log the expected and actual readings
        logging.info(
            f"'{name}': Expected elapsed time = {expected_elapsed_ms:.2f} ms, "
            f"Stored reading = {stored_reading} ms."
        )

        # Assert that the stored_reading matches elapsed_ms exactly
        self.assertEqual(
            stored_reading,
            expected_elapsed_ms,
            msg=(
                f"Stored reading {stored_reading} ms does not exactly match "
                f"elapsed time {expected_elapsed_ms:.2f} ms for '{name}'."
            )
        )

        # Assert that the stored_name matches name exactly
        self.assertEqual(
            stored_name,
            name,
            msg=(
                f"Stored name '{stored_name}' does not match function "
                f"name '{name}'."
            )
        )

    def test_single_function_timing(self):
        """
        Test execution and timing of a single function.
        """
        def sample_function():
            logging.info("Executing sample_function")
            time.sleep(2)  # Simulate processing time
            return True

        with test_logger("test_single_function_timing"):
            self.run_with_timing("sample_function", sample_function)

    def test_multiple_functions_timing(self):
        """
        Test execution and timing of multiple functions within a single test.
        Ensures that each function's timing is recorded separately.
        """
        def function_one():
            logging.info("Executing function_one")
            time.sleep(1)
            return True

        def function_two():
            logging.info("Executing function_two")
            time.sleep(1.5)
            return True

        functions = [function_one, function_two]

        with test_logger("test_multiple_functions_timing"):
            for func in functions:
                self.run_with_timing(func.__name__, func)


    def test_code_block_instrumentation(self):
        """
        Test instrumentation of a specific code block using the Timer context manager.
        Ensures that timing information for the code block is recorded correctly.
        """
        block_name = "sample_code_block"

        def code_block():
            logging.info("Executing sample_code_block with simulated operations")
            logging.info("Step 1: Data retrieval from mock service")
            time.sleep(0.5)  # Simulate network delay
            data = {"user_id": 123, "action": "login"}
            logging.info(f"Data retrieved: {data}")

            logging.info("Step 2: Data processing")
            time.sleep(1)  # Simulate processing time
            processed_data = {k: str(v).upper() for k, v in data.items()}
            logging.info(f"Processed data: {processed_data}")

            logging.info("Step 3: Storing processed data")
            time.sleep(0.5)  # Simulate storage delay
            logging.info("Processed data stored successfully")

        with test_logger("test_code_block_instrumentation"):
            self.run_with_timing(block_name, code_block)

    def test_faulty_function_timing(self):
        """
        Test execution and timing of a faulty function that is expected to raise an exception.
        """
        def faulty_function():
            logging.info("Executing faulty_function")
            time.sleep(1)  # Simulate processing time before failure
            raise ValueError("Simulated error in faulty_function")

        with test_logger("test_faulty_function_timing"):
            try:
                self.run_with_timing("faulty_function", faulty_function)
                self.fail("ValueError was not raised by faulty_function")
            except ValueError as e:
                self.assertEqual(str(e), "Simulated error in faulty_function")