import logging
logging.basicConfig(level=logging.DEBUG)
import gzip
import os 
import copy

import uuid
import json
import emission.storage.json_wrappers as esj
import emission.storage.timeseries.timequery as estt
import emission.storage.timeseries.abstract_timeseries as esta
import emission.storage.timeseries.cache_series as estcs
import emission.net.usercache.abstract_usercache as enua

def get_with_retry(retrieve_call, in_query):
    # Let's clone the query since we are going to modify it
    query = copy.copy(in_query)
    # converts "data.ts" = ["data", "ts"]
    timeTypeSplit = query.timeType.split(".")
    list_so_far = []
    done = False
    while not done:
        # if we don't sort this here, we simply concatenate the entries in the
        # two timeseries and analysis databases so we could end up with a later
        # timestamp from the analysis database as opposed to the timeseries
        (curr_count, curr_batch_cursor) = retrieve_call(query)
        # If this is the first call (as identified by `len(list_so_far) == 0`
        # the count is the total count
        total_count = curr_count if len(list_so_far) == 0 else total_count
        curr_batch = list(curr_batch_cursor)
        if len(list_so_far) > 0 and len(curr_batch) > 0 and curr_batch[0]["_id"] == list_so_far[-1]["_id"]:
            logging.debug(f"first entry {curr_batch[0]['_id']} == last entry so far {list_so_far[-1]['_id']}, deleting")
            del curr_batch[0]
        list_so_far.extend(curr_batch)
        logging.debug(f"Retrieved batch of size {len(curr_batch)}, cumulative {len(list_so_far)} entries of total {total_count} documents for {query}")
        if len(list_so_far) >= total_count:
            done = True
        else:
            query.startTs = curr_batch[-1][timeTypeSplit[0]][timeTypeSplit[1]]
    return list_so_far

def get_from_all_three_sources_with_retry(user_id, in_query, databases=None):
    logging.info("In get_from_all_three_sources_with_retry: Databases = %s" % databases)

    import emission.storage.timeseries.builtin_timeseries as estb

    ts = estb.BuiltinTimeSeries(user_id)
    uc = enua.UserCache.getUserCache(user_id)

    sort_key = ts._get_sort_key(in_query)

    source_db_calls = []

    if databases is None or 'timeseries_db' in databases:
        logging.info("Fetching from timeseries_db")
        base_ts_call = lambda tq: ts._get_entries_for_timeseries(ts.timeseries_db, None, tq,
            geo_query=None, extra_query_list=None, sort_key = sort_key)
        source_db_calls.append(base_ts_call)
    if databases is None or 'analysis_db' in databases:
        logging.info("Fetching from analysis_timeseries_db")
        analysis_ts_call = lambda tq: ts._get_entries_for_timeseries(ts.analysis_timeseries_db, None, tq,
            geo_query=None, extra_query_list=None, sort_key = sort_key)
        source_db_calls.append(analysis_ts_call)
    if databases is None or 'usercache_db' in databases:
        logging.info("Fetching from usercache_db")
        uc_ts_call = lambda tq: (uc.getMessageCount(None, tq), uc.getMessage(None, tq))
        source_db_calls.append(uc_ts_call)

    retry_lists = []
    for source_db_call in source_db_calls:
        retry_lists = retry_lists + get_with_retry(source_db_call, in_query)

    return retry_lists

    '''
    base_ts_call = lambda tq: ts._get_entries_for_timeseries(ts.timeseries_db, None, tq,
        geo_query=None, extra_query_list=None, sort_key = sort_key)
    analysis_ts_call = lambda tq: ts._get_entries_for_timeseries(ts.analysis_timeseries_db, None, tq,
        geo_query=None, extra_query_list=None, sort_key = sort_key)
    uc_ts_call = lambda tq: (uc.getMessageCount(None, tq), uc.getMessage(None, tq))

    return get_with_retry(base_ts_call, in_query) + \
        get_with_retry(analysis_ts_call, in_query) + \
        get_with_retry(uc_ts_call, in_query)
    '''

def get_exported_timeseries_entries(user_id, ts, start_ts, end_ts, databases=None):
    combined_list = []
    entries_lists = {
        "loc_entry_list": None,
        "trip_entry_list": None,
        "place_entry_list": None
    }

    if databases == ['timeseries_db']:
        loc_time_query = estt.TimeQuery("data.ts", start_ts, end_ts)
        loc_entry_list = get_from_all_three_sources_with_retry(user_id, loc_time_query, databases)
        combined_list = loc_entry_list
        logging.info("Found %d loc-like entries = %d total entries" %
            (len(loc_entry_list), len(combined_list)))
        entries_lists["loc_entry_list"] = loc_entry_list
    else:
        loc_time_query = estt.TimeQuery("data.ts", start_ts, end_ts)
        loc_entry_list = get_from_all_three_sources_with_retry(user_id, loc_time_query)
        # Changing to estcs so that we will read the manual entries, which have data.start_ts and data.enter_ts
        # from the usercache as well
        trip_time_query = estt.TimeQuery("data.start_ts", start_ts, end_ts)
        trip_entry_list = get_from_all_three_sources_with_retry(user_id, trip_time_query)
        place_time_query = estt.TimeQuery("data.enter_ts", start_ts, end_ts)
        place_entry_list = get_from_all_three_sources_with_retry(user_id, place_time_query)
        # Handle the case of the first place, which has no enter_ts and won't be
        # matched by the default query
        first_place_extra_query = {'$and': [{'data.enter_ts': {'$exists': False}},
                                            {'data.exit_ts': {'$exists': True}}]}
        first_place_entry_list = list(ts.find_entries(key_list=None, time_query=None, extra_query_list=[first_place_extra_query]))
        logging.info("First place entry list = %s" % first_place_entry_list)
        combined_list = loc_entry_list + trip_entry_list + place_entry_list + first_place_entry_list
        logging.info("Found %d loc-like entries, %d trip-like entries, %d place-like entries = %d total entries" %
            (len(loc_entry_list), len(trip_entry_list), len(place_entry_list), len(combined_list)))
        entries_lists["loc_entry_list"] = loc_entry_list
        entries_lists["trip_entry_list"] = trip_entry_list
        entries_lists["place_entry_list"] = place_entry_list

    return entries_lists, combined_list
    
def export(user_id, ts, start_ts, end_ts, file_name, ma_bool, databases=None):
    logging.info("In export: Databases = %s" % databases)
    logging.info("Extracting timeline for user %s day %s -> %s and saving to file %s" %
                 (user_id, start_ts, end_ts, file_name))
    
    entries_lists, combined_list = get_exported_timeseries_entries(user_id, ts, start_ts, end_ts, databases)

    validate_truncation(entries_lists["loc_entry_list"], entries_lists["trip_entry_list"], entries_lists["place_entry_list"])

    unique_key_list = set([e["metadata"]["key"] for e in combined_list])
    logging.info("timeline has unique keys = %s" % unique_key_list)
    if len(combined_list) == 0 or unique_key_list == set(['stats/pipeline_time']):
        logging.info("No entries found in range for user %s, skipping save" % user_id)
        return None
    else:
        combined_filename = "%s.gz" % (file_name)
        with gzip.open(combined_filename, "wt") as gcfd:
            json.dump(combined_list,
                gcfd, default=esj.wrapped_default, allow_nan=False, indent=4)
        return combined_list

def validate_truncation(loc_entry_list=None, trip_entry_list=None, place_entry_list=None):
    MAX_LIMIT = 25 * 10000
    if loc_entry_list is not None and len(loc_entry_list) == MAX_LIMIT:
        logging.warning("loc_entry_list length = %d, probably truncated" % len(loc_entry_list))
    if trip_entry_list is not None and len(trip_entry_list) == MAX_LIMIT:
        logging.warning("trip_entry_list length = %d, probably truncated" % len(trip_entry_list))
    if place_entry_list is not None and len(place_entry_list) == MAX_LIMIT:
        logging.warning("place_entry_list length = %d, probably truncated" % len(place_entry_list))
