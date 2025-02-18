from __future__ import unicode_literals
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import
from future import standard_library
standard_library.install_aliases()
from builtins import zip
from builtins import str
from builtins import range
from builtins import *
import logging

import json
import emission.storage.json_wrappers as esj
import argparse

import bin.debug.common as common
import os

import gzip

import emission.storage.timeseries.abstract_timeseries as esta
import emission.core.wrapper.user as ecwu
import emission.core.wrapper.entry as ecwe
import emission.storage.timeseries.cache_series as estcs

args = None

def register_fake_users(prefix, unique_user_list, verbose):
    logging.info("Creating user entries for %d users" % len(unique_user_list))

    format_string = "{0}-%0{1}d".format(prefix, len(str(len(unique_user_list))))
    logging.info("pattern = %s" % format_string)

    for i, uuid in enumerate(unique_user_list):
        username = (format_string % i)
        if verbose is not None and i % verbose == 0:
            logging.info("About to insert mapping %s -> %s" % (username, uuid))
        user = ecwu.User.registerWithUUID(username, uuid)

def register_mapped_users(mapfile, unique_user_list, verbose):
    uuid_entries = json.load(open(mapfile), object_hook=esj.wrapped_object_hook)
    logging.info("Creating user entries for %d users from map of length %d" % (len(unique_user_list), len(mapfile)))

    lookup_map = dict([(eu["uuid"], eu) for eu in uuid_entries])

    for i, uuid in enumerate(unique_user_list):
        username = lookup_map[uuid]["user_email"]
        # TODO: Figure out whether we should insert the entry directly or
        # register this way
        # Pro: will do everything that register does, including creating the profile
        # Con: will insert only username and uuid - id and update_ts will be different
        if verbose is not None and i % verbose == 0:
            logging.info("About to insert mapping %s -> %s" % (username, uuid))
        user = ecwu.User.registerWithUUID(username, uuid)

def get_load_ranges(entries, batch_size):
    start_indices = list(range(0, len(entries), batch_size))
    ranges = list(zip(start_indices, start_indices[1:]))
    ranges.append((start_indices[-1], len(entries)))
    return ranges

def load_pipeline_states(file_prefix, all_uuid_list, continue_on_error, verbose):
    import emission.core.get_database as edb
    import pymongo

    for curr_uuid in all_uuid_list:
        pipeline_filename = "%s_pipelinestate_%s.gz" % (file_prefix, curr_uuid)
        print("Loading pipeline state for %s from %s" % 
            (curr_uuid, pipeline_filename))
        with gzip.open(pipeline_filename) as gfd:
            states = json.load(gfd, object_hook = esj.wrapped_object_hook)
            if verbose:
                logging.debug("Loading states of length %s" % len(states))
            if len(states) > 0:
                try:
                    edb.get_pipeline_state_db().insert_many(states)
                except pymongo.errors.BulkWriteError as e:
                    # print(e.__dict__.keys())
                    # print(e._OperationFailure__details.keys())
                    all_error_codes = list(set([we['code'] for we in e.details['writeErrors']]))
                    if len(all_error_codes) == 1 and all_error_codes[0] == 11000 and continue_on_error:
                        logging.info("ignoring duplicate key error while restoring pipeline state")
                    else:
                        logging.error(e.details['writeErrors'])
                        raise(e)
            else:
                logging.info("No pipeline states found, skipping load")

def post_check(unique_user_list, all_rerun_list):
    import emission.core.get_database as edb
    import numpy as np

    logging.info("For %s users, loaded %s raw entries, %s processed entries and %s pipeline states" %
        (len(unique_user_list),
         edb.get_timeseries_db().count_documents({"user_id": {"$in": list(unique_user_list)}}),
         edb.get_analysis_timeseries_db().count_documents({"user_id": {"$in": list(unique_user_list)}}),
         edb.get_pipeline_state_db().count_documents({"user_id": {"$in": list(unique_user_list)}})))

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

def load_multi_timeline_for_range(file_prefix, info_only=None, verbose=None, continue_on_error=None, mapfile=None, prefix=None, batch_size=10000, raw_timeseries_only=False):
    fn = file_prefix
    logging.info("Loading file or prefix %s" % fn)
    sel_file_list = common.read_files_with_prefix(fn)

    all_user_list = []
    all_rerun_list = []
    (tsdb_count, ucdb_count) = (0,0)

    for i, filename in enumerate(sel_file_list):
        if "pipelinestate" in filename:
            continue
        logging.info("=" * 50)
        logging.info("Loading data from file %s" % filename)
        
        entries = json.load(gzip.open(filename), object_hook = esj.wrapped_object_hook)

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

        load_ranges = get_load_ranges(entries, batch_size)
        if not info_only:
            for j, curr_range in enumerate(load_ranges):
                if verbose is not None and j % verbose == 0:
                    logging.info("About to load range %s -> %s" % (curr_range[0], curr_range[1]))
                wrapped_entries = [ecwe.Entry(e) for e in entries[curr_range[0]:curr_range[1]]]
                (tsdb_count, ucdb_count) = estcs.insert_entries(curr_uuid, wrapped_entries, continue_on_error)
        logging.debug("For uuid %s, finished loading %d entries into the usercache and %d entries into the timeseries" % (curr_uuid, ucdb_count, tsdb_count))

    unique_user_list = set(all_user_list)
    if not info_only:
        if not raw_timeseries_only:
            load_pipeline_states(file_prefix, unique_user_list, continue_on_error, verbose)
        if mapfile is not None:
            register_mapped_users(mapfile, unique_user_list, verbose)
        elif prefix is not None:
            register_fake_users(prefix, unique_user_list, verbose)
    
    post_check(unique_user_list, all_rerun_list) 
    return (tsdb_count, ucdb_count)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("file_prefix",
        help="the name of the file or file prefix that contains the json representation of the timeline")

    parser.add_argument("-d", "--debug", type=int,
        help="set log level to DEBUG")

    parser.add_argument("-c", "--continue-on-error", default=False, action='store_true',
        help="set log level to DEBUG")

    parser.add_argument("-v", "--verbose", type=int,
        help="after how many lines we should print a status message.")

    parser.add_argument("-i", "--info-only", default=False, action='store_true',
        help="only print entry analysis")

    parser.add_argument("-s", "--batch-size", default=10000, type=int,
        help="batch size to use for the entries")
    
    parser.add_argument("-t", "--raw-timeseries-only", default=False, action='store_true',
        help="load only raw timeseries data; if not set load both raw and analysis timeseries data")

    group = parser.add_mutually_exclusive_group(required=False)
    group.add_argument("-p", "--prefix", default="user",
        help="prefix for the automatically generated usernames. usernames will be <prefix>-001, <prefix>-002...")
    group.add_argument("-m", "--mapfile",
        help="file containing email <-> uuid mapping for the uuids in the dump")

    args = parser.parse_args()
    if args.debug:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.INFO)

    # load_multi_timeline_for_range(args.file_prefix, args.info_only, args.verbose, args.continue_on_error, args.mapfile, args.prefix, args.batch_size)
    load_multi_timeline_for_range(args.file_prefix, args.info_only, args.verbose, args.continue_on_error, args.mapfile, args.prefix, args.batch_size, args.raw_timeseries_only)
