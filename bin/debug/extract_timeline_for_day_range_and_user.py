# Exports all data for the particular user for the particular day
# Used for debugging issues with trip and section generation 
import sys
import logging
logging.basicConfig(level=logging.DEBUG)

import uuid
import datetime as pydt
import json
import bson.json_util as bju
import arrow

import emission.storage.timeseries.abstract_timeseries as esta
import emission.storage.timeseries.timequery as estt

def export_timeline(user_id_str, start_day_str, end_day_str, file_name):
    logging.info("Extracting timeline for user %s day %s -> %s and saving to file %s" %
                 (user_id_str, start_day_str, end_day_str, file_name))

    # day_dt = pydt.datetime.strptime(day_str, "%Y-%m-%d").date()
    start_day_ts = arrow.get(start_day_str).timestamp
    end_day_ts = arrow.get(end_day_str).timestamp
    logging.debug("start_day_ts = %s (%s), end_day_ts = %s (%s)" % 
        (start_day_ts, arrow.get(start_day_ts),
         end_day_ts, arrow.get(end_day_ts)))

    if user_id_str == "all":
        ts = esta.TimeSeries.get_aggregate_time_series(uuid.UUID(user_id_str))
    else:
        ts = esta.TimeSeries.get_time_series(uuid.UUID(user_id_str))
    loc_time_query = estt.TimeQuery("data.ts", start_day_ts, end_day_ts)
    loc_entry_list = list(ts.find_entries(key_list=None, time_query=loc_time_query))
    trip_time_query = estt.TimeQuery("data.start_ts", start_day_ts, end_day_ts)
    trip_entry_list = list(ts.find_entries(key_list=None, time_query=trip_time_query))
    place_time_query = estt.TimeQuery("data.enter_ts", start_day_ts, end_day_ts)
    place_entry_list = list(ts.find_entries(key_list=None, time_query=place_time_query))

    logging.info("Found %d loc entries, %d trip-like entries, %d place-like entries" % 
        (len(loc_entry_list), len(trip_entry_list), len(place_entry_list)))
    json.dump(loc_entry_list + trip_entry_list + place_entry_list,
        open(file_name, "w"), default=bju.default, allow_nan=False, indent=4)

if __name__ == '__main__':
    if len(sys.argv) != 5:
        print "Usage: %s <user> <start_day> <end_day> <file>" % (sys.argv[0])
    else:
        export_timeline(user_id_str=sys.argv[1], start_day_str=sys.argv[2], end_day_str=sys.argv[3], file_name=sys.argv[4])
