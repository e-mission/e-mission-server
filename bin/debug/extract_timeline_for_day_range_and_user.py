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

import uuid
import datetime as pydt
import json
import bson.json_util as bju
import arrow
import argparse
import emission.storage.timeseries.abstract_timeseries as esta
import emission.storage.timeseries.timequery as estt
import emission.storage.decorations.user_queries as esdu
# only needed to read the motion_activity
# https://github.com/e-mission/e-mission-docs/issues/356#issuecomment-520630934
import emission.export.export as eee

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
    eee.export(user_id, ts, start_day_ts, end_day_ts, "%s_%s" % (file_name, user_id), True)
 
    import emission.core.get_database as edb
    pipeline_state_list = list(edb.get_pipeline_state_db().find({"user_id": user_id}))
    logging.info("Found %d pipeline states %s" %
         (len(pipeline_state_list),
           list([ps["pipeline_stage"] for ps in pipeline_state_list])))
    pipeline_filename = "%s_pipelinestate_%s.gz" % (file_name, user_id)
    with gzip.open(pipeline_filename, "wt") as gpfd:
        json.dump(pipeline_state_list,
          gpfd, default=bju.default, allow_nan=False, indent=4)    

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
