from __future__ import unicode_literals
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import
# Standard imports
from future import standard_library
standard_library.install_aliases()
from builtins import *
import logging
import time
from functools import wraps
from typing import Callable, Any

# Our imports
import emission.storage.timeseries.abstract_timeseries as esta
import emission.core.wrapper.entry as ecwe


# metadata format is 

def store_server_api_time(user_id, call, ts, reading):
  store_stats_entry(user_id, "stats/server_api_time", call, ts, reading)

def store_server_api_error(user_id, call, ts, reading):
  store_stats_entry(user_id, "stats/server_api_error", call, ts, reading)

def store_pipeline_time(user_id, stage_string, ts, reading):
  """

  :param user_id: id of the user
  :param stage: string representing a particular time. Typically the stage name,
  but can also be used for sub sections of a stage
  :param ts: timestamp at the time of the reading
  :param reading: the duration of the stage in ms
  :return:
  """
  store_stats_entry(user_id, "stats/pipeline_time", stage_string, ts, reading)

def store_pipeline_error(user_id, stage_string, ts, reading):
  store_stats_entry(user_id, "stats/pipeline_error", stage_string, ts, reading)

def store_stats_entry(user_id, metadata_key, name, ts, reading):
  data = {
    "name": name,
    "ts": ts,
    "reading": reading
  }
  new_entry = ecwe.Entry.create_entry(user_id, metadata_key, data)
  return esta.TimeSeries.get_time_series(user_id).insert(new_entry)

def store_function_time(user_id: str, stage_string: str, ts: float, reading: float):
    """
    Stores the execution time of a function.

    Parameters:
    - user_id (str): The ID of the user.
    - stage_string (str): The name of the function being timed.
    - ts (float): The timestamp when the function execution started.
    - reading (float): The duration of the function execution in milliseconds.

    Returns:
    - InsertResult: The result of the insert operation.
    """
    store_stats_entry(user_id, "stats/function_time", stage_string, ts, reading)


def time_and_store_function(user_id: str):
    """
    Decorator to measure execution time of functions and store the stats under 'stats/function_time'.

    Parameters:
    - user_id (str): The ID of the user associated with the stats.

    Usage:
    @time_and_store_function(user_id="user123")
    def my_function(...):
        ...
    """
    def decorator(func: Callable):
        @wraps(func)
        def wrapper(*args, **kwargs):
            print(f"Decorator invoked for {func.__name__}")
            stage_string = func.__name__
            ts = time.time()
            logging.info(f"Starting '{stage_string}' execution.")
            start_time = time.time()
            try:
                result = func(*args, **kwargs)
                success = True
                return result
            except Exception as e:
                success = False
                logging.error(f"Error in '{stage_string}': {e}", exc_info=True)
                raise
            finally:
                end_time = time.time()
                duration_ms = (end_time - start_time) * 1000  # Convert to milliseconds
                logging.info(f"Finished '{stage_string}' in {duration_ms:.2f} ms.")
                # Store the timing stats
                try:
                    store_function_time(
                        user_id=user_id,
                        stage_string=stage_string,
                        ts=ts,
                        reading=duration_ms
                    )
                except Exception as storage_error:
                    logging.error(f"Failed to store timing stats for '{stage_string}': {storage_error}", exc_info=True)
                if not success:
                    logging.warning(f"'{stage_string}' encountered an error.")
        return wrapper
    return decorator

