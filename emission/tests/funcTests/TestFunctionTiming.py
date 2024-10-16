# emission/tests/funcTests/TestFunctionTiming.py

import unittest
import logging
import time
import typing as t

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
        logging.basicConfig(level=logging.INFO, format='%(message)s')
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
        logging.info("\n########### Starting test_function_no_delay ###########")
        self.execute_and_time_function(test_function_no_delay)
        logging.info("########### Finished test_function_no_delay ###########\n")

    def test_function_short_delay(self):
        """
        Test execution and timing of test_function_2.
        """
        logging.info("\n########### Starting test_function_short_delay ###########")
        self.execute_and_time_function(test_function_short_delay)
        logging.info("########### Finished test_function_short_delay ###########\n")
    
    def test_function_long_delay(self):
        """
        Test execution and timing of test_function_3.
        """
        logging.info("\n########### Starting test_function_long_delay ###########")
        self.execute_and_time_function(test_function_long_delay)
        logging.info("########### Finished test_function_long_delay ###########\n")

    def test_function_faulty(self):
        """
        Test execution and timing of test_function_faulty, which is expected to raise an exception.
        """
        logging.info("\n########### Starting test_function_faulty ###########")
        with self.assertRaises(ValueError) as context:
            self.execute_and_time_function(test_function_faulty)
        self.assertIn("Simulated error in test_function_faulty", str(context.exception))
        logging.info("########### Finished test_function_faulty ###########\n")
    
    def test_multiple_functions(self):
        """
        Test execution and timing of multiple functions within a single test.
        This ensures that each function's timing is recorded separately.
        """
        logging.info("\n########### Starting test_multiple_functions ###########")
        functions = [
            test_function_no_delay,
            test_function_short_delay,
            test_function_long_delay
        ]
        for func in functions:
            logging.info(f"About to execute and time function: {func.__name__}")
            try:
                self.execute_and_time_function(func)
                logging.info(f"Successfully executed and timed function: {func.__name__}")
            except AssertionError as ae:
                logging.error(f"AssertionError for function '{func.__name__}': {ae}")
                raise
            except Exception as e:
                logging.error(f"Unexpected error for function '{func.__name__}': {e}")
                raise

        # Verification: Retrieve all 'stats/dashboard_time' entries
        logging.info("Retrieving all 'stats/dashboard_time' entries from the database.")
        stored_documents_chain = self.timeseries_db.find_entries(["stats/dashboard_time"], time_query=None)
        stored_documents = list(stored_documents_chain)
        logging.info(f"Number of 'stats/dashboard_time' entries retrieved: {len(stored_documents)}")

        # Assert that the number of stored documents matches the number of functions tested
        expected_count = len(functions)
        actual_count = len(stored_documents)
        logging.info(f"Expected number of timing entries: {expected_count}, Actual: {actual_count}")
        self.assertEqual(
            actual_count,
            expected_count,
            f"Expected {expected_count} timing entries, found {actual_count}."
        )

        # Verify each function's entry
        function_names = {func.__name__ for func in functions}
        stored_names = {doc['data']['name'] for doc in stored_documents if 'name' in doc['data']}
        logging.info(f"Function names tested: {function_names}")
        logging.info(f"Function names stored: {stored_names}")
        self.assertTrue(
            function_names.issubset(stored_names),
            "Not all function timings were recorded correctly."
        )
        logging.info("########### Finished test_multiple_functions ###########\n")

    def test_code_block_instrumentation(self):
        """
        Test instrumentation of specific code blocks using the Timer context manager.
        This ensures that timing information for code blocks is recorded correctly.
        The code block now includes calls to additional helper functions to simulate realistic operations.
        """
        logging.info("\n########### Starting test_code_block_instrumentation ###########")
        block_name = "realistic_sample_code_block"
        logging.info(f"Starting timing for code block: {block_name}")
        
        try:
            with ec_timer.Timer() as timer:
                # Sample code block to be timed with additional complexity
                logging.info("Executing 'realistic' sample code block with helper functions")
                data = self.helper_function_1()
                processed_data = self.helper_function_2(data)
                self.helper_function_3(processed_data)
        
            elapsed_ms = timer.elapsed * 1000  # Convert to milliseconds
        
            # Store the execution time for the code block
            sdq.store_dashboard_time(
                code_fragment_name=block_name,
                timer=timer
            )
            logging.info(f"Code block '{block_name}' executed successfully in {elapsed_ms:.2f} ms.")
        
            # Verification: Retrieve the timing entry for the code block
            stored_documents_chain = self.timeseries_db.find_entries(["stats/dashboard_time"], time_query=None)
            stored_documents = list(stored_documents_chain)
        
            # Filter documents for the specific code block
            block_documents = [
                doc for doc in stored_documents
                if doc['data']['name'] == block_name
            ]
        
            # Assert that the code block timing was recorded
            self.assertTrue(
                len(block_documents) > 0,
                f"Timing data for code block '{block_name}' was not found in the database."
            )
        
            # Verify the stored timing information
            stored_doc = block_documents[-1]  # Get the latest entry
            stored_reading = stored_doc['data']['reading']
            
            self.assertEqual(
                stored_reading,
                elapsed_ms,
                msg=(
                    f"Stored reading {stored_reading} ms does not match "
                    f"expected elapsed time {elapsed_ms:.2f} ms for code block '{block_name}'."
                )
            )
        
            self.assertEqual(
                stored_doc['data']['name'],
                block_name,
                f"Stored name '{stored_doc['data']['name']}' does not match code block name '{block_name}'."
            )
        
        except Exception as e:
            # Store the error timing for the code block
            elapsed_seconds = timer.elapsed if 'timer' in locals() else 0
            elapsed_ms = elapsed_seconds * 1000
            sdq.store_dashboard_error(
                code_fragment_name=block_name,
                timer=timer
            )
            logging.error(f"Code block '{block_name}' failed after {elapsed_ms:.2f} ms with error: {e}")
        
            # Verification: Retrieve the error entry for the code block
            stored_error_chain = self.timeseries_db.find_entries(["stats/dashboard_error"], time_query=None)
            stored_errors = list(stored_error_chain)
        
            # Filter errors for the specific code block
            block_errors = [
                err for err in stored_errors
                if err['data']['name'] == block_name
            ]
        
            # Assert that the error was recorded
            self.assertTrue(
                len(block_errors) > 0,
                f"Error data for code block '{block_name}' was not found in the database."
            )
        
            # Re-raise the exception to let the test fail
            raise
        logging.info("########### Finished test_code_block_instrumentation ###########\n")


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
            logging.info(f"Function '{function_name}' executed successfully in {elapsed_ms:.2f} ms.")

            # Verification: Query for the specific function's timing entry
            stored_documents_chain = self.timeseries_db.find_entries(
                ["stats/dashboard_time"],
                extra_query_list=[{"data.name": function_name}]
            )

            # Convert the chain to a list to make it subscriptable and to allow multiple accesses
            stored_documents = list(stored_documents_chain)

            # Assert that exactly one document was retrieved for the function
            self.assertEqual(
                len(stored_documents),
                1,
                f"Expected exactly 1 timing entry for '{function_name}', found {len(stored_documents)}."
            )
            stored_document = stored_documents[0]
            # Iterate over each document and inspect its contents
            try:
                stored_ts = stored_document['data']['ts']
                stored_reading = stored_document['data']['reading']
                stored_name = stored_document['data']['name']
                logging.info(
                    f"Stored Document for '{function_name}': ts={stored_ts}, reading={stored_reading} ms, name={stored_name}"
                )
            except KeyError as e:
                self.fail(
                    f"Missing key {e} in stored document for '{function_name}'."
                )
            
            # Log the expected and actual readings
            logging.info(
                f"Function '{function_name}': Expected elapsed time = {elapsed_ms:.2f} ms, "
                f"Stored reading = {stored_reading} ms."
            )

            # Assert that the stored_reading matches elapsed_ms exactly
            self.assertEqual(
                stored_reading,
                elapsed_ms,
                msg=(
                    f"Stored reading {stored_reading} ms does not exactly match "
                    f"elapsed time {elapsed_ms:.2f} ms for '{function_name}'."
                )
            )

            # Assert that the stored_name matches function_name exactly
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
            logging.error(f"Function '{function_name}' failed after {elapsed_ms:.2f} ms with error: {e}")

            # Verification: Query for the specific function's error entry
            stored_error_chain = self.timeseries_db.find_entries(
                ["stats/dashboard_error"],
                extra_query_list=[{"data.name": function_name}]
            )

            # Convert the chain to a list to make it subscriptable and to allow multiple accesses
            stored_errors = list(stored_error_chain)

            # Assert that exactly one error document was retrieved for the function
            self.assertEqual(
                len(stored_errors),
                1,
                f"Expected exactly 1 error entry for '{function_name}', found {len(stored_errors)}."
            )
            stored_error = stored_errors[0]
            # Iterate over each document and inspect its contents
            try:
                stored_ts_error = stored_error['data']['ts']
                stored_reading_error = stored_error['data']['reading']
                stored_name_error = stored_error['data']['name']
                logging.info(
                    f"Stored Error Document for '{function_name}': ts={stored_ts_error}, reading={stored_reading_error} ms, name={stored_name_error}"
                )
            except KeyError as e:
                self.fail(
                    f"Missing key {e} in stored error document for '{function_name}'."
                )
            # Log the expected and actual error readings
            logging.info(
                f"Function '{function_name}': Expected elapsed time = {elapsed_ms:.2f} ms, "
                f"Stored error reading = {stored_reading_error} ms."
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
        
    def helper_function_1(self):
        """
        Helper function to perform data retrieval from a mock database.
        Simulates processing time with a sleep.
        """
        logging.info("Executing helper_function_1: Retrieving data from mock database")
        time.sleep(1)  # Simulate processing time
        data = {"key1": "value1", "key2": "value2"}
        logging.info(f"helper_function_1 retrieved data: {data}")
        return data

    def helper_function_2(self, data):
        """
        Helper function to process data retrieved by helper_function_1.
        Simulates processing time with a sleep and performs data transformation.
        """
        logging.info("Executing helper_function_2: Processing retrieved data")
        time.sleep(1.5)  # Simulate processing time
        processed_data = {k: v.upper() for k, v in data.items()}  # Example transformation
        logging.info(f"helper_function_2 processed data: {processed_data}")
        return processed_data

    def helper_function_3(self, processed_data):
        """
        Helper function to save processed data back to the mock database.
        Simulates processing time with a sleep and performs data storage.
        """
        logging.info("Executing helper_function_3: Saving processed data to mock database")
        time.sleep(1.5)  # Simulate processing time
        # Simulate saving data
        logging.info("helper_function_3 saved processed data successfully")
        return True


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
