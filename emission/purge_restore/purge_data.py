# Standard imports 
import logging

# Our imports
import emission.storage.pipeline_queries as espq
import emission.storage.timeseries.abstract_timeseries as esta
import emission.storage.decorations.analysis_timeseries_queries as esda
import emission.purge_restore.export_timeseries as epret
import gzip
import json
import os
import emission.core.get_database as edb
import emission.storage.json_wrappers as esj
import emission.core.wrapper.entry as ecwe
import emission.storage.timeseries.timequery as estt

def purge_data(user_id, archive_dir, export_type):
    file_names = None
    try:
        pdp = PurgeDataPipeline()
        pdp.user_id = user_id
        file_names = pdp.run_purge_data_pipeline(user_id, archive_dir, export_type)
        logging.debug("last_processed_ts with entries_to_export logic = %s" % (pdp.last_processed_ts))
        print("last_processed_ts with entries_to_export logic = %s" % (pdp.last_processed_ts))
        if pdp.last_processed_ts is None:
            logging.debug("After run, last_processed_ts == None, must be early return")
        espq.mark_purge_data_done(user_id, pdp.last_processed_ts)
    except:
        logging.exception("Error while purging timeseries data, timestamp unchanged")
        espq.mark_purge_data_failed(user_id)
    return file_names

class PurgeDataPipeline:
    def __init__(self):
        self._last_processed_ts = None

    @property
    def last_processed_ts(self):
        return self._last_processed_ts

    def run_purge_data_pipeline(self, user_id, archive_dir, export_type):
        ts = esta.TimeSeries.get_time_series(user_id)
        time_query = espq.get_time_range_for_purge_data(user_id)

        if archive_dir is None:
            archive_dir = os.environ.get('DATA_DIR', "emission/archived")

        if os.path.isdir(archive_dir) == False:
            os.mkdir(archive_dir) 

        init_start_ts, init_end_ts = time_query.startTs, time_query.endTs
        logging.info(f"Purge data - Start time: {init_start_ts}, End time: {init_end_ts}")

        file_names = []
        entries_to_export = self.get_export_timeseries_entries(user_id, ts, time_query.startTs, time_query.endTs)
        # count_entries =  len(entries_to_export)

        # If running the pipeline PURGE stage for first time, choose the first timestamp from the timeseries as the starting point 
        # Otherwise cannot add 1 hour (3600 seconds) to a NoneType value if incremental option is selected
        current_start_ts = init_start_ts if init_start_ts is not None else entries_to_export[0]['data']['ts']

        while current_start_ts < init_end_ts:
        # while True:
            print("Inside while loop: current_start_ts = %s" % current_start_ts)
            current_end_ts = min(current_start_ts + 3600, init_end_ts) if export_type == 'incremental' else init_end_ts
            
            print(f"Processing data from {current_start_ts} to {current_end_ts}")

            file_name = archive_dir + "/archive_%s_%s_%s" % (user_id, current_start_ts, current_end_ts)
            export_queries = epret.export(user_id, ts, current_start_ts, current_end_ts, file_name)
            # epret.export(user_id, ts, current_start_ts, current_end_ts, file_name)

            entries_to_export_1 = self.get_export_timeseries_entries(user_id, ts, time_query.startTs, time_query.endTs)
            count_entries_1 =  len(entries_to_export_1)

            if export_queries:
                logging.debug("Exporting to file: %s" % file_name)
                print("Exporting to file: %s" % file_name)
                file_names.append(file_name)
                print("File names: %s" % file_names)

                self.delete_timeseries_entries(user_id, ts, current_start_ts, current_end_ts)

                entries = self.get_export_timeseries_entries(user_id, ts, current_start_ts, current_end_ts)

                if entries:
                    self.last_processed_ts = entries[-1]['data']['ts']
                    logging.debug(f"Exported {len(entries)} entries from {current_start_ts} to {current_end_ts}")
                    logging.debug("Entries exported in timerange %s to %s: %s" % (current_start_ts, current_end_ts, len(entries)))
                    logging.debug("Remaining entries to export: %s" % count_entries_1)
                    logging.debug("Updated last_processed_ts %s" % self._last_processed_ts)
                    print(f"Exported {len(entries)} entries from {current_start_ts} to {current_end_ts}")
                    print("Entries exported in timerange %s to %s: %s" % (current_start_ts, current_end_ts, len(entries)))
                    print("Remaining entries to export: %s" % count_entries_1)
                    print("Updated last_processed_ts %s" % self._last_processed_ts)
            else:
                if count_entries_1 > 0:
                    print("No entries found in current time range from %s to %s" % (current_start_ts, current_end_ts))
                    print("Incrementing time range by 1 hour")
                else:
                    logging.info(f"No entries found from {current_start_ts} to {current_end_ts}")
                    logging.debug("No new data to export, breaking out of while loop")
                    print("No new data to export, breaking out of while loop")
                    break

            current_start_ts = current_end_ts
            if current_start_ts >= init_end_ts:
                break

        print(f"Exported data to {len(file_names)} files")
        print(f"Exported file names: {file_names}")
        return file_names

    # def export_pipeline_states(self, user_id, file_name):
    #     pipeline_state_list = list(edb.get_pipeline_state_db().find({"user_id": user_id}))
    #     logging.info("Found %d pipeline states %s" %
    #         (len(pipeline_state_list),
    #         list([ps["pipeline_stage"] for ps in pipeline_state_list])))
    #     pipeline_filename = "%s_pipelinestate_%s.gz" % (file_name, user_id)
    #     with gzip.open(pipeline_filename, "wt") as gpfd:
    #         json.dump(pipeline_state_list,
    #         gpfd, default=esj.wrapped_default, allow_nan=False, indent=4)    

    def delete_timeseries_entries(self, user_id, ts, start_ts_datetime, end_ts_datetime):
        export_queries = self.get_export_queries(start_ts_datetime, end_ts_datetime)
        for key, value in export_queries.items():
            ts_query = ts._get_query(time_query=value)
            print(ts_query)
            delete_query = {"user_id": user_id, **ts_query}

            count = ts.timeseries_db.count_documents(delete_query)
            logging.debug(f"Number of documents matching for {ts.timeseries_db} with {key} query: {count}")
            print(f"Number of documents matching for {ts.timeseries_db} with {key} query: {count}")

            logging.debug("Deleting entries from database...")
            print("Deleting entries from database...")
            result = ts.timeseries_db.delete_many(delete_query)
            logging.debug(f"Key query: {key}")
            print(f"Key query: {key}")
            logging.debug("{} deleted entries from {} to {}".format(result.deleted_count, start_ts_datetime, end_ts_datetime))
            print("{} deleted entries from {} to {}".format(result.deleted_count, start_ts_datetime, end_ts_datetime))
            
    def get_export_timeseries_entries(self, user_id, ts, start_ts_datetime, end_ts_datetime):
        entries_to_export = []
        export_queries = self.get_export_queries(start_ts_datetime, end_ts_datetime)
        for key, value in export_queries.items():
            tq = value
            sort_key = ts._get_sort_key(tq)
            (ts_db_count, ts_db_result) = ts._get_entries_for_timeseries(ts.timeseries_db, None, tq, geo_query=None, extra_query_list=None, sort_key = sort_key)
            entries_to_export.extend(list(ts_db_result))
            logging.debug(f"Key query: {key}")
            logging.debug("{} fetched entries from {} to {}".format(ts_db_count, start_ts_datetime, end_ts_datetime))
        return entries_to_export
    
    def get_export_queries(self, start_ts, end_ts):
        export_queries = {
            # 'trip_time_query': estt.TimeQuery("data.start_ts", init_start_ts, init_end_ts),
            # 'place_time_query': estt.TimeQuery("data.enter_ts", init_start_ts, init_end_ts),
            'loc_time_query': estt.TimeQuery("data.ts", start_ts, end_ts)
        }
        return export_queries
    