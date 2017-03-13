import logging
import argparse
import json
import bson.json_util as bju

# Our imports
import common
import emission.core.get_database as edb

# This is shamelessly going to use edb because we have immutable data, so no
# way to nicely delete entries through the timeseries options

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    parser = argparse.ArgumentParser()
    parser.add_argument("timeline_filename",
        help="the name of the file that contains the json representation of the timeline")
    parser.add_argument("-v", "--verbose", type=int,
        help="after how many lines we should print a status message.")

    parser.add_argument("-i", "--info-only", default=False, action='store_true',
        help="only print entry analysis")

    parser.add_argument("-s", "--batch-size", default=10000,
        help="batch size to use for the entries")

    parser.add_argument("-p", "--prefix", default="user",
        help="prefix for the automatically generated usernames. usernames will be <prefix>-001, <prefix>-002...")

    args = parser.parse_args()
    fn = args.timeline_filename
    logging.info("Loading file %s" % fn)

    entries = json.load(open(fn), object_hook = bju.object_hook)

    unique_user_list = common.analyse_timeline(entries)
    if not args.info_only:
        ts_db = edb.get_timeseries_db()
        ats_db = edb.get_analysis_timeseries_db()
        udb = edb.get_uuid_db()

        for i, uuid in enumerate(unique_user_list):
            
            logging.info("For uuid = %s, deleting entries from the timeseries" % uuid)
            timeseries_del_result = ts_db.remove({"user_id": uuid})
            logging.info("result = %s" % timeseries_del_result)

            logging.info("For uuid = %s, deleting entries from the analysis_timeseries" % uuid)
            analysis_timeseries_del_result = ats_db.remove({"user_id": uuid})
            logging.info("result = %s" % analysis_timeseries_del_result)

            logging.info("For uuid %s, deleting entries from the user_db" % uuid)
            user_db_del_result = udb.remove({"uuid": uuid})
            logging.info("result = %s" % user_db_del_result)
