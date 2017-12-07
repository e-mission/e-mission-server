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

