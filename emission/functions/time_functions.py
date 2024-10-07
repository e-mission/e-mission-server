import json
import logging
import logging.config
import os
import time
import numpy as np
import arrow
import pymongo

import emission.core.get_database as edb
import emission.core.timer as ect

import emission.storage.decorations.stats_queries as esds

def run_function_pipeline(process_number, function_list, skip_if_no_new_data=False):
    """
    Run the function pipeline with the specified process number and function list.
    Note that the process_number is only really used to customize the log file name
    We could avoid passing it in by using the process id - os.getpid() instead, but
    then we won't get the nice RotatingLogHandler properties such as auto-deleting
    files if there are too many. Maybe it will work properly with logrotate? Need to check

    :param process_number: id representing the process number. In range (0..n)
    :param function_list: the list of functions that this process will handle
    :param skip_if_no_new_data: flag to skip function execution based on custom logic
    :return: None
    """
    try:
        with open("conf/log/function_pipeline.conf", "r") as cf:
            pipeline_log_config = json.load(cf)
    except FileNotFoundError:
        with open("conf/log/function_pipeline.conf.sample", "r") as cf:
            pipeline_log_config = json.load(cf)

    # Customize log filenames with process number
    pipeline_log_config["handlers"]["file"]["filename"] = \
        pipeline_log_config["handlers"]["file"]["filename"].replace("function_pipeline", f"function_pipeline_{process_number}")
    pipeline_log_config["handlers"]["errors"]["filename"] = \
        pipeline_log_config["handlers"]["errors"]["filename"].replace("function_pipeline", f"function_pipeline_{process_number}")

    logging.config.dictConfig(pipeline_log_config)
    np.random.seed(61297777)

    logging.info(f"Processing function list: { [func.__name__ for func in function_list] }")

    for func in function_list:
        func_name = func.__name__
        if func is None:
            continue

        try:
            run_function_pipeline_step(func, skip_if_no_new_data)
        except Exception as e:
            esds.store_function_error(func_name, "WHOLE_PIPELINE", time.time(), None)
            logging.exception(f"Found error {e} while processing pipeline for function {func_name}, skipping")

def run_function_pipeline_step(func, skip_if_no_new_data):
    """
    Execute a single step in the function pipeline.

    :param func: The function to execute
    :param skip_if_no_new_data: Flag to determine if the function should be skipped based on custom logic
    :return: None
    """
    func_name = func.__name__

    with ect.Timer() as timer:
        logging.info(f"********** Function {func_name}: Starting execution **********")
        print(f"{arrow.now()} ********** Function {func_name}: Starting execution **********")
        result = func()

    # Store the execution time
    esds.store_function_time(func_name, "EXECUTION",
                             time.time(), timer.elapsed)

    if skip_if_no_new_data and not result:
        print(f"No new data for {func_name}, and skip_if_no_new_data = {skip_if_no_new_data}, skipping the rest of the pipeline")
        return
    else:
        print(f"Function {func_name} executed with result = {result} and skip_if_no_new_data = {skip_if_no_new_data}, continuing")

    logging.info(f"********** Function {func_name}: Completed execution **********")
