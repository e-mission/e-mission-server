from __future__ import unicode_literals
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import
import time
# Standard imports
from future import standard_library
standard_library.install_aliases()
from builtins import *

# Our imports
import emission.storage.timeseries.abstract_timeseries as esta
import emission.core.wrapper.entry as ecwe
import emission.core.timer as ec_timer


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

def store_dashboard_time(code_fragment_name: str, timer: ec_timer.Timer):
    """
    Stores statistics about execution times in dashboard code using a Timer object.
    Both of our current dashboards generate _aggregate_ metrics. We do not work at a per-user level
    in the Python dashboards, so we pass in only the name of the step being instrumented and the timing information.
    
    :param code_fragment_name (str): The name of the function or code fragment being timed.
    :param timer (ec_timer.Timer): The Timer object that records the execution duration.
    """
    # Get the current timestamp in seconds since epoch
    timestamp = time.time()

    # Call the existing store_stats_entry function
    store_stats_entry(
        user_id=None,  # No user ID as per current dashboard design
        metadata_key="stats/dashboard_time",
        name=code_fragment_name,
        ts=timestamp,
        reading=timer.elapsed_ms
    )


def store_dashboard_error(code_fragment_name: str, timer: ec_timer.Timer):
    # Get the current timestamp in seconds since epoch
    timestamp = time.time()

    # Call the existing store_stats_entry function
    store_stats_entry(
        user_id=None,  # No user ID as per current dashboard design
        metadata_key="stats/dashboard_error",
        name=code_fragment_name,
        ts=timestamp,
        reading=timer.elapsed_ms
    )

