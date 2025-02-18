# Standard imports 
import logging

# Our imports
import emission.storage.pipeline_queries as espq
import emission.storage.timeseries.abstract_timeseries as esta
import emission.storage.decorations.analysis_timeseries_queries as esda
import gzip
import json
import os
import emission.core.get_database as edb
import emission.storage.json_wrappers as esj
import emission.core.wrapper.entry as ecwe
import emission.storage.timeseries.timequery as estt
import emission.export.export as eee

def purge_data(user_id, archive_dir, export_type):
    file_names = None
    try:
        pdp = PurgeDataPipeline()
        pdp.user_id = user_id
        file_names = pdp.run_purge_data_pipeline(user_id, archive_dir, export_type)
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

        initStartTs = time_query.startTs
        initEndTs = time_query.endTs
        logging.debug("Initial pipeline purge query range = start_time: %s , end_time: %s" % (initStartTs, initEndTs))

        file_names = []
        total_entries_to_export = eee.get_exported_timeseries_entries(user_id, ts, time_query.startTs, time_query.endTs, ['timeseries_db'])[1]

        # If running the pipeline PURGE stage for first time, choose the first timestamp from the timeseries as the starting point 
        # Otherwise cannot add 1 hour (3600 seconds) to a NoneType value if incremental option is selected
        current_start_ts = initStartTs if initStartTs is not None else total_entries_to_export[0]['data']['ts']

        while True:
            logging.debug("Inside while loop: current_start_ts = %s" % current_start_ts)
            current_end_ts = min(current_start_ts + 3600, initEndTs) if export_type == 'incremental' else initEndTs

            if export_type == 'incremental':
                current_end_ts = min(current_start_ts + 3600, initEndTs)
                logging.debug("Performing incremental export, increasing current_end_ts: %s" % current_end_ts)
            elif export_type == 'full':
                current_end_ts = initEndTs
                logging.debug("Performing full export, setting current_end_ts to current time: %s" % current_end_ts)
            else:
                raise ValueError("Unknown export_type %s" % export_type)
            
            logging.debug(f"Processing current batch from {current_start_ts} to {current_end_ts}")

            file_name = archive_dir + "/archive_%s_%s_%s" % (user_id, current_start_ts, current_end_ts)
            current_batch_exported_entries = eee.export(user_id, ts, current_start_ts, current_end_ts, file_name, False, ['timeseries_db'])

            # Recompute total entries from pipeline initial start time to end time since we are deleting entries iteratively
            # This is used to keep a track of remaining entries to export
            remaining_timeseries_entries = eee.get_exported_timeseries_entries(user_id, ts, time_query.startTs, time_query.endTs, ['timeseries_db'])[1]

            if current_batch_exported_entries is None and len(remaining_timeseries_entries) > 0:
                logging.debug("No entries found in current time range from %s to %s" % (current_start_ts, current_end_ts))
                logging.debug("Incrementing time range by 1 hour to process remaining timeseries entries")
                current_start_ts = current_end_ts
                continue
            elif current_batch_exported_entries is None and len(remaining_timeseries_entries) == 0:
                logging.debug("No new data to export, breaking out of while loop")
                break
            
            logging.debug("Exported to file: %s" % file_name)
            file_names.append(file_name)
            logging.debug("List of exported file names: %s" % file_names)

            self.delete_timeseries_entries(user_id, ts, current_start_ts, current_end_ts)

            logging.debug("Total entries to export: %s" % len(total_entries_to_export))
            logging.debug("Entries exported in timerange %s to %s: %s" % (current_start_ts, current_end_ts, len(current_batch_exported_entries)))
            logging.debug("Remaining entries to export: %s" % len(remaining_timeseries_entries))
            
            self._last_processed_ts = current_batch_exported_entries[-1]['data']['ts']
            logging.debug("Updated last_processed_ts to last entry in current export batch = %s" % self._last_processed_ts)

            current_start_ts = current_end_ts
            if current_start_ts >= initEndTs:
                break

        logging.debug("Exported data to %s files: %s" % (len(file_names), file_names))
        return file_names
    

    def delete_timeseries_entries(self, user_id, ts, start_ts_datetime, end_ts_datetime):
        export_timequery = estt.TimeQuery("data.ts", start_ts_datetime, end_ts_datetime)
        ts_query = ts._get_query(time_query=export_timequery)
        delete_query = {"user_id": user_id, **ts_query}

        count_entries_to_delete = ts.timeseries_db.count_documents(delete_query)
        logging.debug(f"Number of matching entries for deletion = {count_entries_to_delete}")

        logging.debug("Deleting entries from database...")
        result = ts.timeseries_db.delete_many(delete_query)
        assert(result.deleted_count == count_entries_to_delete)
        logging.debug("{} deleted entries from {} to {}".format(result.deleted_count, start_ts_datetime, end_ts_datetime))

    '''
    def delete_timeseries_entries(self, user_id, ts, start_ts_datetime, end_ts_datetime):
        export_queries = self.get_export_queries(start_ts_datetime, end_ts_datetime)
        for key, value in export_queries.items():
            ts_query = ts._get_query(time_query=value)
            logging.debug(ts_query)
            delete_query = {"user_id": user_id, **ts_query}

            count = ts.timeseries_db.count_documents(delete_query)
            logging.debug(f"Number of documents matching for {ts.timeseries_db} with {key} query: {count}")

            logging.debug("Deleting entries from database...")
            result = ts.timeseries_db.delete_many(delete_query)
            logging.debug(f"Key query: {key}")
            logging.debug("{} deleted entries from {} to {}".format(result.deleted_count, start_ts_datetime, end_ts_datetime))

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
            # 'trip_time_query': estt.TimeQuery("data.start_ts", initStartTs, initEndTs),
            # 'place_time_query': estt.TimeQuery("data.enter_ts", initStartTs, initEndTs),
            'loc_time_query': estt.TimeQuery("data.ts", start_ts, end_ts)
        }
        return export_queries
    '''
