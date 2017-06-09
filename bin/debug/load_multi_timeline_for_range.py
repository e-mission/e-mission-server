import logging

import json
import bson.json_util as bju
import argparse

import common
import os

import gzip

import emission.storage.timeseries.abstract_timeseries as esta
import emission.core.wrapper.user as ecwu
import emission.core.wrapper.entry as ecwe

args = None

def register_fake_users(prefix, unique_user_list):
    logging.info("Creating user entries for %d users" % len(unique_user_list))

    format_string = "{0}-%0{1}d".format(prefix, len(str(len(unique_user_list))))
    logging.info("pattern = %s" % format_string)

    for i, uuid in enumerate(unique_user_list):
        username = (format_string % i)
        if args.verbose is not None and i % args.verbose == 0:
            logging.info("About to insert mapping %s -> %s" % (username, uuid))
        # Let's register and then update instead of manipulating the database directly
        # Alternatively, we can refactor register to pass in the uuid as well
        user = ecwu.User.registerWithUUID(username, uuid)

def get_load_ranges(entries):
    start_indices = range(0, len(entries), args.batch_size)
    ranges = zip(start_indices, start_indices[1:])
    ranges.append((start_indices[-1], len(entries)))
    return ranges

def post_check(unique_user_list, all_rerun_list):
    import emission.core.get_database as edb
    import numpy as np

    logging.info("For %s users, loaded %s raw entries and %s processed entries" %
        (len(unique_user_list),
         edb.get_timeseries_db().find({"user_id": {"$in": list(unique_user_list)}}).count(),
         edb.get_analysis_timeseries_db().find({"user_id": {"$in": list(unique_user_list)}}).count()))

    all_rerun_arr = np.array(all_rerun_list)
   
    # want to check if no entry needs a rerun? In this case we are done
    # no entry needs a rerun = all entries are false, not(all entries) are true
    if np.all(np.logical_not(all_rerun_list)):
        logging.info("all entries in the timeline contain analysis results, no need to run the intake pipeline")
    # if all entries need to be re-run, we must have had raw data throughout
    elif np.all(all_rerun_list):
        logging.info("all entries in the timeline contain only raw data, need to run the intake pipeline")
    else:
        logging.info("timeline contains a mixture of analysis results and raw data - complain to shankari!")

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("timeline_filename",
        help="the name of the file or file prefix that contains the json representation of the timeline")

    parser.add_argument("-d", "--debug", type=int,
        help="set log level to DEBUG")

    parser.add_argument("-v", "--verbose", type=int,
        help="after how many lines we should print a status message.")

    parser.add_argument("-i", "--info-only", default=False, action='store_true',
        help="only print entry analysis")

    parser.add_argument("-s", "--batch-size", default=10000,
        help="batch size to use for the entries")

    parser.add_argument("-p", "--prefix", default="user",
        help="prefix for the automatically generated usernames. usernames will be <prefix>-001, <prefix>-002...")

    args = parser.parse_args()
    if args.debug:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.INFO)

    fn = args.timeline_filename
    logging.info("Loading file or prefix %s" % fn)
    sel_file_list = common.read_files_with_prefix(fn)

    all_user_list = []
    all_rerun_list = []

    for i, filename in enumerate(sel_file_list):
        logging.info("=" * 50)
        logging.info("Loading data from file %s" % filename)
        
        entries = json.load(gzip.open(filename), object_hook = bju.object_hook)

        # Obtain uuid and rerun information from entries
        curr_uuid_list, needs_rerun = common.analyse_timeline(entries)
        if len(curr_uuid_list) > 1:
            logging.warning("Found %d users, %s in filename, aborting! " % 
                (len(curr_uuid_list), curr_uuid_list))
            raise RuntimeException("Found %d users, %s in filename, expecting 1, %s" %
                (len(curr_uuid_list), curr_uuid_list, common.split_user_id(filename)))
        curr_uuid = curr_uuid_list[0]
        all_user_list.append(curr_uuid)
        all_rerun_list.append(needs_rerun)

        load_ranges = get_load_ranges(entries)
        if not args.info_only:
            ts = esta.TimeSeries.get_time_series(curr_uuid)
            for j, curr_range in enumerate(load_ranges):
                if args.verbose is not None and j % args.verbose == 0:
                    logging.info("About to load range %s -> %s" % (curr_range[0], curr_range[1]))
                wrapped_entries = [ecwe.Entry(e) for e in entries[curr_range[0]:curr_range[1]]]
                for entry in wrapped_entries:
                    insert_result = ts.insert(entry)

    unique_user_list = set(all_user_list)
    if not args.info_only:
        register_fake_users(args.prefix, unique_user_list)
       
    post_check(unique_user_list, all_rerun_list) 
