import logging
import argparse
import uuid
from datetime import datetime
import emission.core.wrapper.user as ecwu
import emission.core.get_database as edb
import emission.core.wrapper.pipelinestate as ecwp
import emission.storage.pipeline_queries as esp
import pandas as pd
from bson import ObjectId
import json
from uuid import UUID
import tempfile
from datetime import datetime
import emission.storage.timeseries.abstract_timeseries as esta
import emission.storage.pipeline_queries as espq
import emission.export.export as eee

DEFAULT_DIR_NAME = tempfile.gettempdir()
DEFAULT_FILE_PREFIX = "old_timeseries_"

def exportOldTimeseriesAsCsv(user_id, all_data, filename):
    logging.info("Exporting data to CSV...")
    current_datetime = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    filename += "_" + current_datetime + ".csv"
    all_df = pd.json_normalize(all_data)
    all_df.to_csv(filename)
    logging.info("Old timeseries data exported as CSV to {}".format(filename))

def exportOldTimeseriesAsJson(user_id, all_data, filename):
    logging.info("Exporting data to JSON...")
    def custom_encoder(obj):
        if isinstance(obj, (UUID, ObjectId)):
            return str(obj)
        raise TypeError(f"Type {type(obj)} not serializable")

    current_datetime = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    filename += "_" + current_datetime + ".json"
    with open(filename, 'w') as file:
        json.dump(all_data, file, default=custom_encoder, indent=4)
    logging.info("Old timeseries data exported as JSON to {}".format(filename))

exportOptions = {
    'json_export': exportOldTimeseriesAsJson,
    'csv_export': exportOldTimeseriesAsCsv
}

# def purgeUserTimeseries(exportFileFlags, user_uuid, user_email=None, dir_name=DEFAULT_DIR_NAME, file_prefix=DEFAULT_FILE_PREFIX, unsafe_ignore_save=False):
#     if user_uuid:
#         user_id = uuid.UUID(user_uuid)
#     else:
#         user_id = ecwu.User.fromEmail(user_email).uuid

#     cstate = esp.get_current_state(user_id, ecwp.PipelineStages.CREATE_CONFIRMED_OBJECTS)
    
#     if cstate is None:
#         logging.info(f"No matching pipeline state found for {user_id}, purging aborted.")
#     else:
#         last_ts_run = cstate['last_ts_run']
#         logging.info(f"last_ts_run : {last_ts_run}")

#         if not last_ts_run:
#             logging.warning("No processed timeseries for user {}, purging aborted".format(user_id))
#             exit(1)

#         filename = dir_name + "/" + file_prefix + str(user_id)
#         logging.info("Querying data...")
#         all_data = list(edb.get_timeseries_db().find({"user_id": user_id, "metadata.write_ts": { "$lt": last_ts_run}}))

#         print(len(all_data))

#         if len(all_data) == 0:
#             logging.info("No matching data found for the user, purging aborted")
#         else:
#             logging.info("Fetched data, starting export")
#             if unsafe_ignore_save is True:
#                 logging.warning("CSV export was ignored")
#             else: 
#                 for key in exportFileFlags:
#                     logging.info(f"{key} = {exportFileFlags[key]}")
#                     if exportFileFlags[key] is True:
#                         exportOptions[key](user_id, all_data, filename)

#             # logging.info("Deleting entries from database...")
#             # result = edb.get_timeseries_db().delete_many({"user_id": user_id, "metadata.write_ts": { "$lt": last_ts_run}})
#             # logging.info("{} deleted entries since {}".format(result.deleted_count, datetime.fromtimestamp(last_ts_run)))
    


# def purgeUserTimeseries(user_uuid, user_email=None, databases=None, dir_name=DEFAULT_DIR_NAME, unsafe_ignore_save=False):
def purgeUserTimeseries(user_uuid, user_email=None, dir_name=DEFAULT_DIR_NAME, unsafe_ignore_save=False):
    if user_uuid:
        user_id = uuid.UUID(user_uuid)
    else:
        user_id = ecwu.User.fromEmail(user_email).uuid

    ts = esta.TimeSeries.get_time_series(user_id)
    
    print("user_id: ", user_id)
    
    if unsafe_ignore_save is True:
        logging.warning("CSV export was ignored")
    else: 
        logging.info("Fetched data, starting export")    
        # epe.run_export_pipeline("single", [user_id], databases, dir_name)
        
        ts = esta.TimeSeries.get_time_series(user_id)
        time_query = espq.get_time_range_for_export_data(user_id)
        export_file_name = dir_name + "/archive_%s_%s_%s" % (user_id, time_query.startTs, time_query.endTs)

        
        start_ts_datetime = datetime.datetime.fromtimestamp(time_query.startTs).strftime('%Y-%m-%d %H:%M:%S')
        end_ts_datetime = datetime.datetime.fromtimestamp(time_query.endTs).strftime('%Y-%m-%d %H:%M:%S')
        print("Start Time: ", start_ts_datetime)
        print("Start Ts: ", time_query.startTs)
        print("End Time: ", end_ts_datetime)
        print("End Ts: ", time_query.endTs)

        # Receiving these queries from export.py that were used to fetch the data entries that were exported.
        # Need these for use in the purge_user_timeseries.py script so that we only delete those entries that were exported
        export_queries = eee.export(user_id, ts, time_query.startTs, time_query.endTs, export_file_name, False, databases=['timeseries_db'])

        for key, value in export_queries.items():
            if value["type"] == "time":
                ts_query = ts._get_query(time_query=value["query"])
                print(ts_query)
            # Separate case for handling the first_place_extra_query from export.py
            # else: 
            #     ts_query = ts._get_query(extra_query_list=[value["query"]])
            #     print(ts_query)
            #     sort_key = ts._get_sort_key(None)
            #     print(len(list(ts.find_entries(key_list=None, time_query=None, extra_query_list=[value["query"]]))))
            delete_query = {"user_id": user_id, **ts_query}

            # Get the count of matching documents
            count = ts.timeseries_db.count_documents(delete_query)
            print(f"Number of documents matching for {ts.timeseries_db} with {key} query: {count}")
            # print(f"Number of documents deleted for {ts.timeseries_db} with {key} query: {deleted_count}")

            print("Deleting entries from database...")
            # result = edb.get_timeseries_db().delete_many({"user_id": user_id, "metadata.write_ts": { "$lt": last_ts_run}})
            result = ts.timeseries_db.delete_many(delete_query)
            print(f"Key query: {key}")
            print("{} deleted entries from {} to {}".format(result.deleted_count, start_ts_datetime, end_ts_datetime))

        return file_name


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
    # exportFileFlags = {
    #     'json_export': True,
    #     'csv_export': args.csv_export if args.csv_export is not None else False
    # }
    logging.info(f"Default temporary directory: {DEFAULT_DIR_NAME}")
    # purgeUserTimeseries(exportFileFlags, args.user_uuid, args.user_email, args.dir_name, args.file_prefix, args.unsafe_ignore_save)
    purgeUserTimeseries(args.user_uuid, args.user_email, args.dir_name, args.unsafe_ignore_save)