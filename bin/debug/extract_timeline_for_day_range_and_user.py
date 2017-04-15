# Exports all data for the particular user for the particular day
# Used for debugging issues with trip and section generation 
import sys
import logging
logging.basicConfig(level=logging.DEBUG)
import gzip

import uuid
import datetime as pydt
import json
import bson.json_util as bju
import arrow

import emission.storage.timeseries.abstract_timeseries as esta
import emission.storage.timeseries.timequery as estt
import emission.storage.decorations.user_queries as esdu

def export_timeline(user_id, start_day_str, end_day_str, file_name):
    logging.info("Extracting timeline for user %s day %s -> %s and saving to file %s" %
                 (user_id, start_day_str, end_day_str, file_name))

    # day_dt = pydt.datetime.strptime(day_str, "%Y-%m-%d").date()
    start_day_ts = arrow.get(start_day_str).timestamp
    end_day_ts = arrow.get(end_day_str).timestamp
    logging.debug("start_day_ts = %s (%s), end_day_ts = %s (%s)" % 
        (start_day_ts, arrow.get(start_day_ts),
         end_day_ts, arrow.get(end_day_ts)))

    ts = esta.TimeSeries.get_time_series(user_id)
    loc_time_query = estt.TimeQuery("data.ts", start_day_ts, end_day_ts)
    loc_entry_list = list(ts.find_entries(key_list=None, time_query=loc_time_query))
    trip_time_query = estt.TimeQuery("data.start_ts", start_day_ts, end_day_ts)
    trip_entry_list = list(ts.find_entries(key_list=None, time_query=trip_time_query))
    place_time_query = estt.TimeQuery("data.enter_ts", start_day_ts, end_day_ts)
    place_entry_list = list(ts.find_entries(key_list=None, time_query=place_time_query))

    combined_list = loc_entry_list + trip_entry_list + place_entry_list
    logging.info("Found %d loc entries, %d trip-like entries, %d place-like entries = %d total entries" % 
        (len(loc_entry_list), len(trip_entry_list), len(place_entry_list), len(combined_list)))

    validate_truncation(loc_entry_list, trip_entry_list, place_entry_list)

    unique_key_list = set(map(lambda e: e["metadata"]["key"], combined_list))
    logging.info("timeline has unique keys = %s" % unique_key_list)
    if len(combined_list) == 0 or unique_key_list == set(['stats/pipeline_time']):
        logging.info("No entries found in range for user %s, skipping save" % user_id)
    else:
        combined_filename = "%s_%s.gz" % (file_name, user_id)
        json.dump(combined_list,
            gzip.open(combined_filename, "wb"), default=bju.default, allow_nan=False, indent=4)

def validate_truncation(loc_entry_list, trip_entry_list, place_entry_list):
    MAX_LIMIT = 25 * 10000
    if len(loc_entry_list) == MAX_LIMIT:
        logging.warning("loc_entry_list length = %d, probably truncated" % len(loc_entry_list))
    if len(trip_entry_list) == MAX_LIMIT:
        logging.warning("trip_entry_list length = %d, probably truncated" % len(trip_entry_list))
    if len(place_entry_list) == MAX_LIMIT:
        logging.warning("place_entry_list length = %d, probably truncated" % len(place_entry_list))


if __name__ == '__main__':
    if len(sys.argv) != 5:
        print "Usage: %s <user> <start_day> <end_day> <file_prefix>" % (sys.argv[0])
    else:
        user_id_str = sys.argv[1]
        if user_id_str == "all":
            all_uuids = esdu.get_all_uuids()
            for curr_uuid in all_uuids:
                if curr_uuid != '':
                    logging.info("=" * 50)
                    export_timeline(user_id=curr_uuid, start_day_str=sys.argv[2], end_day_str=sys.argv[3], file_name=sys.argv[4])
        else:
            export_timeline(user_id=uuid.UUID(sys.argv[1]), start_day_str=sys.argv[2], end_day_str=sys.argv[3], file_name=sys.argv[4])
