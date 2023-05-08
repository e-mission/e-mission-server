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

def get_sensed_mode_fractions(ct):
    # These keys were found in emission/core/wrapper/modeprediction.py:
    sensed_mode_types = {0: "unknown", 1: "walking",2: "bicycling",
                     3: "bus", 4: "train", 5: "car", 6: "air_or_hsr",
                     7: "subway", 8: "tram", 9: "light_rail"}

    # Get the segments for the trip.
    #cleaned_section will only have walk/bike/automotive, inferred_section is the one that has bus/train/car etc 
    segments = esdt.get_sections_for_trip(key = "analysis/inferred_section", user_id = ct["user_id"], trip_id = ct['data']['cleaned_trip'])

    # get pairs of mode type and duration
    trip_mode_durations = {}
    total_dur = 0
    for s in segments:
        # the sensed mode is a number in the database, so I'm relabeling it as a string.
        mode = sensed_mode_types[s['data']['sensed_mode']]
        duration = s['data']['duration']

        if mode not in trip_mode_durations.keys(): trip_mode_durations[mode] = 0
        trip_mode_durations[mode] += duration

        total_dur += duration
    # convert the durations to fractions of the total segment moving time (not the trip time, since trips include stop times)
    return {mode: duration/total_dur  for mode,duration in trip_mode_durations.items()}

def export_trip_table_as_csv(user_id, start_day_str, end_day_str, timezone, fp):
    logging.info("Extracting trip list for user %s day %s -> %s and saving to file %s" %
                 (user_id, start_day_str, end_day_str, fp))

    start_day_ts = arrow.get(start_day_str).replace(tzinfo=timezone).timestamp()
    end_day_ts = arrow.get(end_day_str).replace(tzinfo=timezone).timestamp()
    logging.debug("start_day_ts = %s (%s), end_day_ts = %s (%s)" % 
        (start_day_ts, arrow.get(start_day_ts).to(timezone),
         end_day_ts, arrow.get(end_day_ts).to(timezone)))

    ts = esta.TimeSeries.get_time_series(user_id)
    trip_time_query = estt.TimeQuery("data.start_ts", start_day_ts, end_day_ts)
    ct_df = ts.get_data_df("analysis/confirmed_trip", trip_time_query)
    if len(ct_df) > 0:
        ct_df["sensed_mode"] = ct_df.apply(lambda row: get_sensed_mode_fractions(ts.df_row_to_entry("analysis/confirmed_trip", row)), axis=1)
        expanded_ct_df = esdt.expand_userinputs(ct_df)
        expanded_ct_df.to_csv(fp)

def export_trip_tables_as_csv(user_id_list, args):
    trip_table_fp = open(args.file_prefix+"_trip_table.csv", "w")
    for curr_uuid in user_id_list:
        if curr_uuid != '':
            logging.info("=" * 50)
            export_trip_table_as_csv(curr_uuid, start_day_str = args.start_day, end_day_str = args.end_day,
                timezone=args.timezone, fp=trip_table_fp)
    trip_table_fp.close()

def export_demographic_table_as_csv(uuid_list, args):
    print("Looking up details for %s" % uuid_list)
    all_survey_results = list(edb.get_timeseries_db().find({"user_id": {"$in": uuid_list}, "metadata.key": "manual/demographic_survey"}))
    for s in all_survey_results:
        s["data"]["user_id"] = s["user_id"]
    all_survey_results_df = pd.json_normalize([s["data"] for s in all_survey_results])
    all_survey_results_df.drop(columns=['xmlResponse', 'name', 'version', 'label'], axis=1, inplace=True)
    all_survey_results_df.to_csv(args.file_prefix+"_demographic_table.csv")

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
    export_demographic_table_as_csv(uuid_list, args)

