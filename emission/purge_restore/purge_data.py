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

def purge_data(user_id, archive_dir):
    try:
        pdp = PurgeDataPipeline()
        pdp.user_id = user_id
        file_name = pdp.run_purge_data_pipeline(user_id, archive_dir)
        logging.debug("last_processed_ts with entries_to_export logic = %s" % (pdp.last_processed_ts))
        if pdp.last_processed_ts is None:
            logging.debug("After run, last_processed_ts == None, must be early return")
        espq.mark_purge_data_done(user_id, pdp.last_processed_ts)
    except:
        logging.exception("Error while purging timeseries data, timestamp unchanged")
        espq.mark_purge_data_failed(user_id)
    return file_name

class PurgeDataPipeline:
    def __init__(self):
        self._last_processed_ts = None

    @property
    def last_processed_ts(self):
        return self._last_processed_ts

    def run_purge_data_pipeline(self, user_id, archive_dir):
        ts = esta.TimeSeries.get_time_series(user_id)
        time_query = espq.get_time_range_for_purge_data(user_id)

        initEndTs = time_query.endTs
        logging.debug("Initial created: time_query.endTs = %s" % initEndTs)

        print("Inside: purge_data - Start time: %s" % time_query.startTs)
        print("Inside: purge_data - End time: %s" % time_query.endTs)
        if archive_dir is None:
            if "DATA_DIR" in os.environ:
                archive_dir = os.environ['DATA_DIR']
            else:
                archive_dir = "emission/archived"

        if os.path.isdir(archive_dir) == False:
            os.mkdir(archive_dir) 

        # time_query.endTs = entries[-1]['metadata']['write_ts']
        # logging.debug("Updated from export data file: time_query.endTs = %s" % time_query.endTs)

        file_name = archive_dir + "/archive_%s_%s_%s" % (user_id, time_query.startTs, time_query.endTs)
        print("Exporting to file: %s" % file_name)
        
        export_queries = epret.export(user_id, ts, time_query.startTs, time_query.endTs, file_name, False)

        if export_queries is None:
            logging.debug("No data to export, export_queries is None")
        else:
            entries_to_export = self.get_exported_timeseries_entries(user_id, ts, time_query.startTs, time_query.endTs, export_queries)
            self.export_pipeline_states(user_id, file_name)
            self.delete_timeseries_entries(user_id, ts, time_query.startTs, time_query.endTs, export_queries)

            if len(entries_to_export) == 0:
                # Didn't process anything new so start at the same point next time
                self._last_processed_ts = None
            else:  
                self._last_processed_ts = entries_to_export[-1]['data']['ts']

        return file_name

    def export_pipeline_states(self, user_id, file_name):
        pipeline_state_list = list(edb.get_pipeline_state_db().find({"user_id": user_id}))
        logging.info("Found %d pipeline states %s" %
            (len(pipeline_state_list),
            list([ps["pipeline_stage"] for ps in pipeline_state_list])))
        pipeline_filename = "%s_pipelinestate_%s.gz" % (file_name, user_id)
        with gzip.open(pipeline_filename, "wt") as gpfd:
            json.dump(pipeline_state_list,
            gpfd, default=esj.wrapped_default, allow_nan=False, indent=4)    

    def delete_timeseries_entries(self, user_id, ts, start_ts_datetime, end_ts_datetime, export_queries):
        for key, value in export_queries.items():
            ts_query = ts._get_query(time_query=value)
            print(ts_query)
            delete_query = {"user_id": user_id, **ts_query}

            # Get the count of matching documents
            count = ts.timeseries_db.count_documents(delete_query)
            logging.debug(f"Number of documents matching for {ts.timeseries_db} with {key} query: {count}")

            logging.debug("Deleting entries from database...")
            result = ts.timeseries_db.delete_many(delete_query)
            logging.debug(f"Key query: {key}")
            logging.debug("{} deleted entries from {} to {}".format(result.deleted_count, start_ts_datetime, end_ts_datetime))

    def get_exported_timeseries_entries(self, user_id, ts, start_ts_datetime, end_ts_datetime, export_queries):
        entries_to_export = []
        for key, value in export_queries.items():
            tq = value
            sort_key = ts._get_sort_key(tq)
            (ts_db_count, ts_db_result) = ts._get_entries_for_timeseries(ts.timeseries_db, None, tq, geo_query=None, extra_query_list=None, sort_key = sort_key)
            entries_to_export.extend(list(ts_db_result))
            logging.debug(f"Key query: {key}")
            logging.debug("{} fetched entries from {} to {}".format(ts_db_count, start_ts_datetime, end_ts_datetime))
        return entries_to_export
