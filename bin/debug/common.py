from __future__ import unicode_literals
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import
from future import standard_library
standard_library.install_aliases()
from builtins import *
import logging

import glob

def analyse_timeline(entries):
    logging.info("Analyzing timeline...")
    logging.info("timeline has %d entries" % len(entries))

    unique_user_list = set([e["user_id"] for e in entries])
    logging.info("timeline has data from %d users" % len(unique_user_list))
    unique_user_list_list = list(unique_user_list)
    blank_uuid_list = [entry for entry in entries if entry["user_id"] == '']
    if len(blank_uuid_list) > 0:
        logging.info("Found %d entries with blank uuid, loading them anyway" % 
            (len(blank_uuid_list)))

    unique_key_list = set([e["metadata"]["key"] for e in entries])
    logging.info("timeline has the following unique keys %s" % unique_key_list)

    if "analysis/cleaned_trip" in unique_key_list and "analysis/cleaned_place" in unique_key_list:
        logging.info("timeline for user %s contains analysis results" % unique_user_list_list[0])
        needs_rerun = False
    else:
        logging.info("timeline for user %s contains only raw data" % unique_user_list_list[0])
        needs_rerun = True

    return unique_user_list_list, needs_rerun

split_user_id = lambda fn: fn.split("_")[-1].split(".")[0]

def read_files_with_prefix(prefix):
    matching_files = glob.glob(prefix+"*")
    logging.info("Found %d matching files for prefix %s" % (len(matching_files), prefix))
    logging.info("files are %s ... %s" % (matching_files[0:5], matching_files[-5:-1]))
    return matching_files
