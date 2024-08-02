# Standard imports 
import logging

# Our imports
import emission.storage.pipeline_queries as espq
import emission.storage.timeseries.abstract_timeseries as esta
import emission.storage.decorations.analysis_timeseries_queries as esda
import gzip
import json
import os
import bin.debug.load_multi_timeline_for_range as lmtfr
import emission.storage.json_wrappers as esj

def restore_data(user_id, file_name):
    try:
        rdp = RestoreDataPipeline()
        rdp.user_id = user_id
        rdp.run_restore_data_pipeline(user_id, file_name)
        if rdp.last_processed_ts is None:
            logging.debug("After run, last_trip_done == None, must be early return")
        espq.mark_restore_data_done(user_id, rdp.last_processed_ts)
    except:
        logging.exception("Error while restoring timeseries data, timestamp unchanged")
        espq.mark_restore_data_failed(user_id)

class RestoreDataPipeline:
    def __init__(self):
        self._last_processed_ts = None

    @property
    def last_processed_ts(self):
        return self._last_processed_ts

    def run_restore_data_pipeline(self, user_id, file_name):
        time_query = espq.get_time_range_for_restore_data(user_id)
        entries = json.load(gzip.open(file_name + ".gz"), object_hook = esj.wrapped_object_hook)
        self._last_processed_ts = entries[-1]['metadata']['write_ts']
        lmtfr.load_multi_timeline_for_range(file_prefix=file_name, continue_on_error=True)
