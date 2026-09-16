import logging
import argparse
import uuid
from datetime import datetime
import emission.core.wrapper.user as ecwu
import emission.core.get_database as edb
import emission.core.wrapper.pipelinestate as ecwp
import emission.core.wrapper.pipelinestate as ecwp
import emission.storage.pipeline_queries as esp
import pandas as pd


DEFAULT_DIR_NAME = "/tmp"
DEFAULT_FILE_PREFIX = "old_timeseries_"

def exportOldTimeseriesAsCsv(user_id, last_ts_run, dir_name, file_prefix):
    filename = dir_name + "/" + file_prefix + str(user_id) + ".csv"
    all_data = list(edb.get_timeseries_db().find({"user_id": user_id, "metadata.write_ts": { "$lt": last_ts_run}}))
    all_df = pd.json_normalize(all_data)
    all_df.to_csv(filename)
    logging.info("Old timeseries data exported to {}".format(filename))

def purgeUserTimeseries(user_uuid, user_email=None, dir_name=DEFAULT_DIR_NAME, file_prefix=DEFAULT_FILE_PREFIX, unsafe_ignore_save=False):
    if user_uuid:
        user_id = uuid.UUID(user_uuid)
    else:
        user_id = ecwu.User.fromEmail(user_email).uuid

    cstate = esp.get_current_state(user_id, ecwp.PipelineStages.CREATE_CONFIRMED_OBJECTS)
    last_ts_run = cstate['last_ts_run']

    if not last_ts_run:
        logging.warning("No processed timeserie for user {}".format(user_id))
        exit(1)

    if unsafe_ignore_save is True:
        logging.warning("CSV export was ignored")
    else:
        exportOldTimeseriesAsCsv(user_id, last_ts_run, dir_name, file_prefix)

    res = edb.get_timeseries_db().delete_many({"user_id": user_id, "metadata.write_ts": { "$lt": last_ts_run}})
    logging.info("{} deleted entries since {}".format(res.deleted_count, datetime.fromtimestamp(last_ts_run)))
    
if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)

    parser = argparse.ArgumentParser(prog="purge_user_timeseries")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("-e", "--user_email")
    group.add_argument("-u", "--user_uuid")
    parser.add_argument(
        "-d", "--dir_name", 
        help="Target directory for exported csv data (defaults to {})".format(DEFAULT_DIR_NAME), 
        default=DEFAULT_DIR_NAME
    )
    parser.add_argument(
        "--file_prefix", 
        help="File prefix for exported csv data (defaults to {})".format(DEFAULT_FILE_PREFIX), 
        default=DEFAULT_FILE_PREFIX
    )
    parser.add_argument(
        "--unsafe_ignore_save", 
        help="Ignore csv export of deleted data (not recommended, this operation is definitive)",
        action='store_true'
    )

    args = parser.parse_args()
    purgeUserTimeseries(args.user_uuid, args.user_email, args.dir_name, args.file_prefix, args.unsafe_ignore_save)