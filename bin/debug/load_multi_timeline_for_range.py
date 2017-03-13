import logging

import json
import bson.json_util as bju
import emission.storage.timeseries.abstract_timeseries as esta

import argparse

import common
import emission.core.wrapper.user as ecwu
import emission.core.wrapper.entry as ecwe

args = None

def register_fake_users(prefix, unique_user_list):
    logging.info("Creating user entries for %d users" % len(unique_user_list))

    format_string = "{0}-%0{1}d".format(prefix, len(str(len(unique_user_list))))
    logging.debug("pattern = %s" % format_string)

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

def post_check(unique_user_list):
    import emission.core.get_database as edb

    logging.info("For %s users, loaded %s raw entries and %s processed entries" %
        (len(unique_user_list),
         edb.get_timeseries_db().find({"user_id": {"$in": list(unique_user_list)}}).count(),
         edb.get_analysis_timeseries_db().find({"user_id": {"$in": list(unique_user_list)}}).count()))

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
    ts = esta.TimeSeries.get_aggregate_time_series()

    entries = json.load(open(fn), object_hook = bju.object_hook)

    unique_user_list = common.analyse_timeline(entries)
    if not args.info_only:
        register_fake_users(args.prefix, unique_user_list)
        load_ranges = get_load_ranges(entries)

        for i, curr_range in enumerate(load_ranges):
            if args.verbose is not None and i % args.verbose == 0:
                logging.info("About to load range %s -> %s" % (curr_range[0], curr_range[1]))
            wrapped_entries = [ecwe.Entry(e) for e in entries[curr_range[0]:curr_range[1]]]
            for entry in wrapped_entries:
                insert_result = ts.insert(entry)
       
        post_check(unique_user_list) 
