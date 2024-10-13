# emission/tests/funcTests/TestFunctionTiming.py

import logging
import time
from typing import Callable, List

# Import the store_dashboard_time and store_dashboard_error functions
from emission.storage.decorations.stats_queries import (
    store_dashboard_time,
    store_dashboard_error
)

# Import the existing Timer context manager
from emission.core.timer import Timer as ECT_Timer


# Define test functions
def test_function_1():
    logging.info("Executing test_function_1")
    time.sleep(1)  # Simulate processing time
    return True  # Indicate successful execution

def test_function_2():
    logging.info("Executing test_function_2")
    time.sleep(2)
    return True

def test_function_faulty():
    logging.info("Executing test_function_faulty")
    time.sleep(1)
    raise ValueError("Simulated error in test_function_faulty")

def test_function_3():
    logging.info("Executing test_function_3")
    time.sleep(3)
    return True

def execute_and_time_function(func: Callable[[], bool]):
    """
    Executes a given function, measures its execution time using ECT_Timer,
    and stores the timing information using store_dashboard_time.
    If the function raises an exception, it stores the error using store_dashboard_error.

    Parameters:
    - func (Callable[[], bool]): The test function to execute and time.
    """
    function_name = func.__name__
    timestamp = time.time()

    logging.info(f"Starting timing for function: {function_name}")

    try:
        with ECT_Timer() as timer:
            result = func()  # Execute the test function

        elapsed_seconds = timer.elapsed  # Accessing the float attribute directly
        elapsed_ms = elapsed_seconds * 1000  # Convert to milliseconds

        # Store the execution time
        store_dashboard_time(
            code_fragment_name=function_name,
            ts=timestamp,
            reading=elapsed_ms
        )
        print(f"Function '{function_name}' executed successfully in {elapsed_ms:.2f} ms.")
        logging.info(f"Function '{function_name}' executed successfully in {elapsed_ms:.2f} ms.")

    except Exception as e:
        # Even if the function fails, capture the elapsed time up to the exception
        elapsed_seconds = timer.elapsed if 'timer' in locals() else 0  # Accessing the float attribute directly
        elapsed_ms = elapsed_seconds * 1000

        # Store the error timing
        store_dashboard_error(
            code_fragment_name=function_name,
            ts=timestamp,
            reading=elapsed_ms
        )
        print(f"Function '{function_name}' failed after {elapsed_ms:.2f} ms with error: {e}")
        logging.error(f"Function '{function_name}' failed after {elapsed_ms:.2f} ms with error: {e}")

def main():
    # Define the list of test functions, including the faulty one
    function_list: List[Callable[[], bool]] = [
        test_function_1,
        test_function_2,
        # test_function_faulty,  # This will raise an exception
        test_function_3  # This should execute normally after the faulty function
    ]
    # Execute and time each function
    for func in function_list:
        execute_and_time_function(func)

if __name__ == "__main__":
    main()
