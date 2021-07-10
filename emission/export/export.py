import logging
logging.basicConfig(level=logging.DEBUG)
import gzip
import os 

import uuid
import json
import bson.json_util as bju
import emission.storage.timeseries.timequery as estt
import emission.storage.timeseries.abstract_timeseries as esta
import emission.storage.timeseries.cache_series as estcs
import emission.net.usercache.abstract_usercache as enua
def export(user_id, ts, start_ts, end_ts, file_name, ma_bool):
    if ma_bool:
        ma_time_query = estt.TimeQuery("metadata.write_ts", start_ts, end_ts)
        uc = enua.UserCache.getUserCache(user_id)
        ma_entry_list = uc.getMessage(["background/motion_activity"], ma_time_query)
    else: 
        ma_entry_list = [] 
    loc_time_query = estt.TimeQuery("data.ts", start_ts, end_ts)
    loc_entry_list = list(estcs.find_entries(user_id, key_list=None, time_query=loc_time_query))
    trip_time_query = estt.TimeQuery("data.start_ts", start_ts, end_ts)
    trip_entry_list = list(ts.find_entries(key_list=None, time_query=trip_time_query))
    place_time_query = estt.TimeQuery("data.enter_ts", start_ts, end_ts)
    place_entry_list = list(ts.find_entries(key_list=None, time_query=place_time_query))
    first_place_extra_query = {'$and': [{'data.enter_ts': {'$exists': False}},{'data.exit_ts': {'$exists': True}}]}
    first_place_entry_list = list(ts.find_entries(key_list=None, time_query=None, extra_query_list=[first_place_extra_query]))
    logging.info("First place entry list = %s" % first_place_entry_list)
    combined_list = ma_entry_list + loc_entry_list + trip_entry_list + place_entry_list + first_place_entry_list
	
    logging.info("Found %d loc entries, %d motion entries, %d trip-like entries, %d place-like entries = %d total entries" %
        (len(loc_entry_list), len(ma_entry_list), len(trip_entry_list), len(place_entry_list), len(combined_list)))
    validate_truncation(loc_entry_list, trip_entry_list, place_entry_list)

    unique_key_list = set([e["metadata"]["key"] for e in combined_list])
    logging.info("timeline has unique keys = %s" % unique_key_list)
    if len(combined_list) == 0 or unique_key_list == set(['stats/pipeline_time']):
        logging.info("No entries found in range for user %s, skipping save" % user_id)
    else:
        combined_filename = "%s.gz" % (file_name)
        with gzip.open(combined_filename, "wt") as gcfd:
            json.dump(combined_list,gcfd, default=bju.default, allow_nan=False, indent=4)

def validate_truncation(loc_entry_list, trip_entry_list, place_entry_list):
    MAX_LIMIT = 25 * 10000
    if len(loc_entry_list) == MAX_LIMIT:
        logging.warning("loc_entry_list length = %d, probably truncated" % len(loc_entry_list))
    if len(trip_entry_list) == MAX_LIMIT:
        logging.warning("trip_entry_list length = %d, probably truncated" % len(trip_entry_list))
    if len(place_entry_list) == MAX_LIMIT:
        logging.warning("place_entry_list length = %d, probably truncated" % len(place_entry_list))
