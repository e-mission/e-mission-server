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

def purge_data(user_id, archive_dir):
    try:
        pdp = PurgeDataPipeline()
        pdp.user_id = user_id
        file_name = pdp.run_purge_data_pipeline(user_id, archive_dir)
        if pdp.last_trip_done is None:
            logging.debug("After run, last_trip_done == None, must be early return")
        espq.mark_purge_data_done(user_id, pdp.last_trip_done)
    except:
        logging.exception("Error while purging timeseries data, timestamp unchanged")
        espq.mark_purge_data_failed(user_id)
    return file_name

class PurgeDataPipeline:
    def __init__(self):
        self._last_trip_done = None

    @property
    def last_trip_done(self):
        return self._last_trip_done

    def run_purge_data_pipeline(self, user_id, archive_dir):
        ts = esta.TimeSeries.get_time_series(user_id)
        time_query = espq.get_time_range_for_purge_data(user_id)
        print("Inside: purge_data - Start time: %s" % time_query.startTs)
        print("Inside: purge_data - End time: %s" % time_query.endTs)
        if archive_dir is None:
            if "DATA_DIR" in os.environ:
                archive_dir = os.environ['DATA_DIR']
            else:
                archive_dir = "emission/archived"

        if os.path.isdir(archive_dir) == False:
            os.mkdir(archive_dir) 
        file_name = archive_dir + "/archive_%s_%s_%s" % (user_id, time_query.startTs, time_query.endTs)
        print("Exporting to file: %s" % file_name)
        export_queries = epret.export(user_id, ts, time_query.startTs, time_query.endTs, file_name, False)
        self.export_pipeline_states(user_id, file_name)
        self.delete_timeseries_entries(user_id, ts, time_query.startTs, time_query.endTs, export_queries)
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
                if value["type"] == "time":
                    ts_query = ts._get_query(time_query=value["query"])
                    print(ts_query)
                delete_query = {"user_id": user_id, **ts_query}

                # Get the count of matching documents
                count = ts.timeseries_db.count_documents(delete_query)
                print(f"Number of documents matching for {ts.timeseries_db} with {key} query: {count}")

                print("Deleting entries from database...")
                # result = edb.get_timeseries_db().delete_many({"user_id": user_id, "metadata.write_ts": { "$lt": last_ts_run}})
                result = ts.timeseries_db.delete_many(delete_query)
                print(f"Key query: {key}")
                print("{} deleted entries from {} to {}".format(result.deleted_count, start_ts_datetime, end_ts_datetime))
