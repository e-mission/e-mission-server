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
import pymongo
from bson import ObjectId
import json
from uuid import UUID

DEFAULT_DIR_NAME = "/tmp"
DEFAULT_FILE_PREFIX = "old_timeseries_"

def exportOldTimeseriesAsCsv(user_id, all_data, filename):
    logging.info("Exporting data to CSV...")
    filename += ".csv"
    all_df = pd.json_normalize(all_data)
    all_df.to_csv(filename)
    logging.info("Old timeseries data exported as CSV to {}".format(filename))

def exportOldTimeseriesAsJson(user_id, all_data, filename):
    logging.info("Exporting data to JSON...")
    def custom_encoder(obj):
        if isinstance(obj, (UUID, ObjectId)):
            return str(obj)
        raise TypeError(f"Type {type(obj)} not serializable")

    filename += ".json"
    with open(filename, 'w') as file:
        json.dump(all_data, file, default=custom_encoder)
    logging.info("Old timeseries data exported as JSON to {}".format(filename))

exportOptions = {
    'json_export': exportOldTimeseriesAsJson,
    'csv_export': exportOldTimeseriesAsCsv
}

def purgeUserTimeseries(exportFileFlags, user_uuid, user_email=None, dir_name=DEFAULT_DIR_NAME, file_prefix=DEFAULT_FILE_PREFIX, unsafe_ignore_save=False):
    if user_uuid:
        user_id = uuid.UUID(user_uuid)
    else:
        user_id = ecwu.User.fromEmail(user_email).uuid

    cstate = esp.get_current_state(user_id, ecwp.PipelineStages.CREATE_CONFIRMED_OBJECTS)
    last_ts_run = cstate['last_ts_run']
    logging.info(f"last_ts_run : {last_ts_run}")

    if not last_ts_run:
        logging.warning("No processed timeseries for user {}".format(user_id))
        exit(1)

    filename = dir_name + "/" + file_prefix + str(user_id)
    logging.info("Querying data...")
    all_data = list(edb.get_timeseries_db().find({"user_id": user_id, "metadata.write_ts": { "$lt": last_ts_run}}))
    logging.info("Fetched data...")

    if unsafe_ignore_save is True:
        logging.warning("CSV export was ignored")
    else: 
        for key in exportFileFlags:
            logging.info(f"{key} = {exportFileFlags[key]}")
            if exportFileFlags[key] is True:
                exportOptions[key](user_id, all_data, filename)

    logging.info("Deleting entries from database...")
    result = edb.get_timeseries_db().delete_many({"user_id": user_id, "metadata.write_ts": { "$lt": last_ts_run}})
    logging.info("{} deleted entries since {}".format(result.deleted_count, datetime.fromtimestamp(last_ts_run)))
    
if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)

    parser = argparse.ArgumentParser(prog="purge_user_timeseries")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("-e", "--user_email")
    group.add_argument("-u", "--user_uuid")
    parser.add_argument(
        "-d", "--dir_name", 
        help="Target directory for exported JSON data (defaults to {})".format(DEFAULT_DIR_NAME), 
        default=DEFAULT_DIR_NAME
    )
    parser.add_argument(
        "--file_prefix", 
        help="File prefix for exported JSON data (defaults to {})".format(DEFAULT_FILE_PREFIX), 
        default=DEFAULT_FILE_PREFIX
    )
    parser.add_argument(
        "--csv_export", 
        help="Exporting to CSV file alongwith default JSON file",
        action='store_true'
    )
    parser.add_argument(
        "--unsafe_ignore_save", 
        help="Ignore export of deleted data (not recommended, this operation is definitive)",
        action='store_true'
    )

    args = parser.parse_args()
    exportFileFlags = {
        'json_export': True,
        'csv_export': args.csv_export if args.csv_export is not None else False
    }
    purgeUserTimeseries(exportFileFlags, args.user_uuid, args.user_email, args.dir_name, args.file_prefix, args.unsafe_ignore_save)