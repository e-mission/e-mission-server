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
from bson.binary import Binary
from bson import ObjectId
import json

def importOldTimeseriesFromJson(filename):
    logging.info("Importing data from JSON...")
    with open(filename, 'r') as file:
        data = json.load(file)

    # Converting _id to ObjectId and UUID string to binary BinData
    for document in data:
        document["_id"] = ObjectId(document["_id"])
        document["user_id"] = Binary(uuid.UUID(document["user_id"]).bytes, 0x03)
    logging.info("Old timeseries data loaded from JSON...")
    return data

importOptions = {
    'json_import': importOldTimeseriesFromJson
}

def restoreUserTimeseries(importFileFlags, filename):
    for key in importFileFlags:
        logging.info(f"{key} = {importFileFlags[key]}")
        if importFileFlags[key] is True:
            data = importOptions[key](filename)
            logging.info("Printing data to be inserted...")
            break

    logging.info("Inserting data into database...")
    result = edb.get_timeseries_db().insert_many(data)
    logging.info("{} documents successfully inserted".format(len(result.inserted_ids)))
    
if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)

    parser = argparse.ArgumentParser(prog="restore_user_timeseries")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "--json_import", 
        help="Importing from JSON file",
        action='store_true'
    )
    parser.add_argument(
        "-f", "--file_name", 
        help="Path to the JSON file containing data to be imported"
    )

    args = parser.parse_args()
    importFileFlags = {
        'json_import': args.json_import if args.json_import is not None else False
    }
    restoreUserTimeseries(importFileFlags, args.file_name)