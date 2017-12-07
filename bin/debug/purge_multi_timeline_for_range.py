from __future__ import unicode_literals
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import
from future import standard_library
standard_library.install_aliases()
from builtins import *
import logging
import argparse
import json
import gzip
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
    logging.info("Loading file or prefix %s" % fn)
    sel_file_list = common.read_files_with_prefix(fn)

    ts_db = edb.get_timeseries_db()
    ats_db = edb.get_analysis_timeseries_db()
    udb = edb.get_uuid_db()

    for i, filename in enumerate(sel_file_list):
        logging.info("=" * 50)
        logging.info("Deleting data from file %s" % filename)

        entries = json.load(gzip.open(filename), object_hook = bju.object_hook)

        # Obtain uuid and rerun information from entries
        curr_uuid_list, needs_rerun = common.analyse_timeline(entries)
        if len(curr_uuid_list) > 1:
            logging.warning("Found %d users, %s in filename, aborting! " % 
                (len(curr_uuid_list), curr_uuid_list))
            raise RuntimeException("Found %d users, %s in filename, expecting 1, %s" %
                (len(curr_uuid_list), curr_uuid_list, common.split_user_id(filename)))
        curr_uuid = curr_uuid_list[0]
        if not args.info_only:
            logging.info("For uuid = %s, deleting entries from the timeseries" % curr_uuid)
            timeseries_del_result = ts_db.remove({"user_id": curr_uuid})
            logging.info("result = %s" % timeseries_del_result)

            logging.info("For uuid = %s, deleting entries from the analysis_timeseries" % curr_uuid)
            analysis_timeseries_del_result = ats_db.remove({"user_id": curr_uuid})
            logging.info("result = %s" % analysis_timeseries_del_result)

            logging.info("For uuid %s, deleting entries from the user_db" % curr_uuid)
            user_db_del_result = udb.remove({"uuid": curr_uuid})
            logging.info("result = %s" % user_db_del_result)
