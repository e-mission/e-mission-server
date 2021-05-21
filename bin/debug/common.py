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

def purge_entries_for_user(curr_uuid, is_purge_state, db_array=None):
    logging.info("For uuid = %s, deleting entries from the timeseries" % curr_uuid)
    if db_array is not None:
        [ts_db, ats_db, udb, psdb] = db_array
        logging.debug("db_array passed in with databases %s" % db_array)
    else:
        import emission.core.get_database as edb

        pr_db = edb.get_profile_db()
        tse_db = edb.get_timeseries_error_db()
        uc_db = edb.get_usercache_db()
        ts_db = edb.get_timeseries_db()
        ats_db = edb.get_analysis_timeseries_db()
        udb = edb.get_uuid_db()
        psdb = edb.get_pipeline_state_db()
        logging.debug("db_array not passed in, looking up databases")

    timeseries_del_result = ts_db.remove({"user_id": curr_uuid})
    logging.info("result = %s" % timeseries_del_result)

    logging.info("For uuid = %s, deleting entries from the profiles" % curr_uuid)
    profiles_del_result = pr_db.remove({"user_id": curr_uuid})
    logging.info("result = %s" % profiles_del_result)

    logging.info("For uuid = %s, deleting entries from the usercache" % curr_uuid)
    usercache_del_result = uc_db.remove({"user_id": curr_uuid})
    logging.info("result = %s" % usercache_del_result)

    logging.info("For uuid = %s, deleting entries from the timeseries_error" % curr_uuid)
    timeseries_error_del_result = tse_db.remove({"user_id": curr_uuid})
    logging.info("result = %s" % timeseries_error_del_result)

    logging.info("For uuid = %s, deleting entries from the analysis_timeseries" % curr_uuid)
    analysis_timeseries_del_result = ats_db.remove({"user_id": curr_uuid})
    logging.info("result = %s" % analysis_timeseries_del_result)

    logging.info("For uuid %s, deleting entries from the user_db" % curr_uuid)
    user_db_del_result = udb.remove({"uuid": curr_uuid})
    logging.info("result = %s" % user_db_del_result)

    if is_purge_state:
        logging.info("For uuid %s, deleting entries from the pipeline_state_db" % curr_uuid)
        psdb_del_result = psdb.remove({"user_id": curr_uuid})
        logging.info("result = %s" % psdb_del_result)
 
