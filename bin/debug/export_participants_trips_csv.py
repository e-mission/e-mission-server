import sys
import logging
logging.basicConfig(level=logging.DEBUG)
import gzip

import uuid
import datetime as pydt
import json
import bson.json_util as bju
import arrow
import argparse

import emission.core.get_database as edb
import emission.core.wrapper.user as ecwu
import emission.storage.timeseries.abstract_timeseries as esta
import emission.storage.timeseries.timequery as estt
import emission.storage.decorations.user_queries as esdu
import emission.storage.decorations.trip_queries as esdt
import pandas as pd

def export_participant_table_as_csv(uuid_list, args):
    print("Looking up details for %s" % uuid_list)
    all_uuid_data = list(edb.get_uuid_db().find({"uuid": {"$in": uuid_list}}))
    all_uuid_df = pd.json_normalize(all_uuid_data)
    all_uuid_df.rename(columns={"user_email": "user_token"}, inplace=True)
    all_uuid_df.to_csv(args.file_prefix+"_participant_table.csv")

def export_trip_table_as_csv(user_id, start_day_str, end_day_str, timezone, file_name):
    logging.info("Extracting trip list for user %s day %s -> %s and saving to file %s" %
                 (user_id, start_day_str, end_day_str, file_name))

    start_day_ts = arrow.get(start_day_str).replace(tzinfo=timezone).timestamp
    end_day_ts = arrow.get(end_day_str).replace(tzinfo=timezone).timestamp
    logging.debug("start_day_ts = %s (%s), end_day_ts = %s (%s)" % 
        (start_day_ts, arrow.get(start_day_ts).to(timezone),
         end_day_ts, arrow.get(end_day_ts).to(timezone)))

    ts = esta.TimeSeries.get_time_series(user_id)
    trip_time_query = estt.TimeQuery("data.start_ts", start_day_ts, end_day_ts)
    ct_df = ts.get_data_df("analysis/confirmed_trip", trip_time_query)
    if len(ct_df) > 0:
        expanded_ct_df = esdt.expand_userinputs(ct_df)
        expanded_ct_df.to_csv(file_name)

def export_trip_tables_as_csv(user_id_list, args):
    for curr_uuid in user_id_list:
        if curr_uuid != '':
            logging.info("=" * 50)
            export_trip_table_as_csv(curr_uuid, start_day_str = args.start_day, end_day_str = args.end_day,
                timezone=args.timezone, file_name=args.file_prefix+"_"+str(curr_uuid)+"_trip_table.csv")

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    parser = argparse.ArgumentParser(prog="export_participants_trips")

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
    export_participant_table_as_csv(uuid_list, args)
    export_trip_tables_as_csv(uuid_list, args)
