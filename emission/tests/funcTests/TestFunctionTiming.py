# emission/tests/funcTests/TestFunctionTiming.py

import json
import logging
import logging.config
import os
import time
import numpy as np
import arrow
from contextlib import contextmanager

# Import the run_function_pipeline function from time_functions.py
from emission.functions.time_functions import run_function_pipeline

# Define test functions
def test_function_1():
    logging.info("Executing test_function_1")
    time.sleep(1)  # Simulate processing time
    return True  # Indicate successful execution

def test_function_2():
    logging.info("Executing test_function_2")
    time.sleep(1)
    return True

def test_function_faulty():
    logging.info("Executing test_function_faulty")
    time.sleep(1)
    raise ValueError("Simulated error in test_function_faulty")

def test_function_3():
    logging.info("Executing test_function_3")
    time.sleep(1)
    return True

if __name__ == "__main__":
    # Ensure the logs directory exists
    os.makedirs("logs", exist_ok=True)
    
    # Define the list of test functions, including the faulty one
    function_list = [
        test_function_1,
        test_function_2,
        test_function_faulty,  # This will raise an exception
        test_function_3  # This should execute normally after the faulty function
    ]
    
    # Run the pipeline with process number 1 and skip_if_no_new_data set to True
    run_function_pipeline(process_number=1, function_list=function_list, skip_if_no_new_data=True)
