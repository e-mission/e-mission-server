# Standard imports
import logging
import time

# Our imports
import emission.storage.timeseries.abstract_timeseries as esta
import emission.core.wrapper.entry as ecwe
from emission.core.get_database import get_client_stats_db_backup, get_server_stats_db_backup, get_result_stats_db_backup


# metadata format is 

def store_server_api_time(user_id, call, ts, reading):
  data = {
    "name": call,
    "ts": ts,
    "reading": reading
  }
  new_entry = ecwe.Entry.create_entry(user_id, "stats/server_api_time", data)
  return esta.TimeSeries.get_time_series(user_id).insert(new_entry)

def store_server_api_error(user_id, call, ts, reading):
  data = {
    "name": call,
    "ts": ts,
    "reading": reading
  }
  new_entry = ecwe.Entry.create_entry(user_id, "stats/server_api_error", data)
  return esta.TimeSeries.get_time_series(user_id).insert(new_entry)

