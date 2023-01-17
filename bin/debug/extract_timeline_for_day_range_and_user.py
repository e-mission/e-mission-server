from __future__ import print_function
from __future__ import unicode_literals
from __future__ import division
from __future__ import absolute_import
# Exports all data for the particular user for the particular day
# Used for debugging issues with trip and section generation 
from future import standard_library
standard_library.install_aliases()
from builtins import *
import sys
import logging
logging.basicConfig(level=logging.DEBUG)
import gzip
import copy

import uuid
import datetime as pydt
import json
import bson.json_util as bju
import arrow
import argparse

import emission.core.wrapper.user as ecwu
import emission.storage.timeseries.abstract_timeseries as esta
import emission.storage.timeseries.timequery as estt
import emission.storage.decorations.user_queries as esdu
import emission.storage.timeseries.cache_series as estcs
# only needed to read the motion_activity
# https://github.com/e-mission/e-mission-docs/issues/356#issuecomment-520630934
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

def get_from_all_three_sources_with_retry(user_id, in_query):
    import emission.storage.timeseries.builtin_timeseries as estb

    ts = estb.BuiltinTimeSeries(user_id)
    uc = enua.UserCache.getUserCache(user_id)

    sort_key = ts._get_sort_key(in_query)
    base_ts_call = lambda tq: ts._get_entries_for_timeseries(ts.timeseries_db, None, tq,
        geo_query=None, extra_query_list=None, sort_key = sort_key)
    analysis_ts_call = lambda tq: ts._get_entries_for_timeseries(ts.analysis_timeseries_db, None, tq,
        geo_query=None, extra_query_list=None, sort_key = sort_key)
    uc_ts_call = lambda tq: (uc.getMessageCount(None, tq), uc.getMessage(None, tq))

    return get_with_retry(base_ts_call, in_query) + \
        get_with_retry(analysis_ts_call, in_query) + \
        get_with_retry(uc_ts_call, in_query)

def export_timeline(user_id, start_day_str, end_day_str, timezone, file_name):
    logging.info("Extracting timeline for user %s day %s -> %s and saving to file %s" %
                 (user_id, start_day_str, end_day_str, file_name))

    # day_dt = pydt.datetime.strptime(day_str, "%Y-%m-%d").date()
    start_day_ts = arrow.get(start_day_str).replace(tzinfo=timezone).timestamp
    end_day_ts = arrow.get(end_day_str).replace(tzinfo=timezone).timestamp
    logging.debug("start_day_ts = %s (%s), end_day_ts = %s (%s)" % 
        (start_day_ts, arrow.get(start_day_ts).to(timezone),
         end_day_ts, arrow.get(end_day_ts).to(timezone)))

    ts = esta.TimeSeries.get_time_series(user_id)
    loc_time_query = estt.TimeQuery("data.ts", start_day_ts, end_day_ts)
    loc_entry_list = get_from_all_three_sources_with_retry(user_id, loc_time_query)
    # Changing to estcs so that we will read the manual entries, which have data.start_ts and data.enter_ts
    # from the usercache as well
    trip_time_query = estt.TimeQuery("data.start_ts", start_day_ts, end_day_ts)
    trip_entry_list = get_from_all_three_sources_with_retry(user_id, trip_time_query)
    place_time_query = estt.TimeQuery("data.enter_ts", start_day_ts, end_day_ts)
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

    validate_truncation(loc_entry_list, trip_entry_list, place_entry_list)

    unique_key_list = set([e["metadata"]["key"] for e in combined_list])
    logging.info("timeline has unique keys = %s" % unique_key_list)
    if len(combined_list) == 0 or unique_key_list == set(['stats/pipeline_time']):
        logging.info("No entries found in range for user %s, skipping save" % user_id)
    else:
        # Also dump the pipeline state, since that's where we have analysis results upto
        # This allows us to copy data to a different *live system*, not just
        # duplicate for analysis
        combined_filename = "%s_%s.gz" % (file_name, user_id)
        with gzip.open(combined_filename, "wt") as gcfd:
            json.dump(combined_list,
                gcfd, default=bju.default, allow_nan=False, indent=4)

        import emission.core.get_database as edb

        pipeline_state_list = list(edb.get_pipeline_state_db().find({"user_id": user_id}))
        logging.info("Found %d pipeline states %s" %
            (len(pipeline_state_list),
             list([ps["pipeline_stage"] for ps in pipeline_state_list])))

        pipeline_filename = "%s_pipelinestate_%s.gz" % (file_name, user_id)
        with gzip.open(pipeline_filename, "wt") as gpfd:
            json.dump(pipeline_state_list,
                gpfd, default=bju.default, allow_nan=False, indent=4)

def validate_truncation(loc_entry_list, trip_entry_list, place_entry_list):
    MAX_LIMIT = 25 * 10000
    if len(loc_entry_list) == MAX_LIMIT:
        logging.warning("loc_entry_list length = %d, probably truncated" % len(loc_entry_list))
    if len(trip_entry_list) == MAX_LIMIT:
        logging.warning("trip_entry_list length = %d, probably truncated" % len(trip_entry_list))
    if len(place_entry_list) == MAX_LIMIT:
        logging.warning("place_entry_list length = %d, probably truncated" % len(place_entry_list))

def export_timeline_for_users(user_id_list, args):
    for curr_uuid in user_id_list:
        if curr_uuid != '':
            logging.info("=" * 50)
            export_timeline(user_id=curr_uuid, start_day_str=args.start_day,
                end_day_str= args.end_day, timezone=args.timezone,
                file_name=args.file_prefix)

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    parser = argparse.ArgumentParser(prog="extract_timeline_for_day_range_and_user")

    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("-e", "--user_email", nargs="+")
    group.add_argument("-u", "--user_uuid", nargs="+")
    group.add_argument("-a", "--all", action="store_true")
    group.add_argument("-f", "--file")

    parser.add_argument("--timezone", default="UTC")
    parser.add_argument("start_day", help="start day in utc - e.g. 'YYYY-MM-DD'" )
    parser.add_argument("end_day", help="start day in utc - e.g. 'YYYY-MM-DD'" )
    parser.add_argument("file_prefix", help="prefix for the filenames generated - e.g /tmp/dump_ will generate files /tmp/dump_<uuid1>.gz, /tmp/dump_<uuid2>.gz..." )

    args = parser.parse_args()

    if args.user_uuid:
        uuid_list = [uuid.UUID(uuid_str) for uuid_str in args.user_uuid]
    elif args.user_email:
        uuid_list = [ecwu.User.fromEmail(uuid_str).uuid for uuid_str in args.user_email]
    elif args.all:
        uuid_list = esdu.get_all_uuids()
    elif args.file:
        with open(args.file) as fd:
            uuid_entries = json.load(fd, object_hook=bju.object_hook)
            uuid_list = [ue["uuid"] for ue in uuid_entries]
    export_timeline_for_users(uuid_list, args)
