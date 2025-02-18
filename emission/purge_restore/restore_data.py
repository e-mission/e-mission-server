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
import emission.core.get_database as edb
import emission.core.wrapper.pipelinestate as ecwp

def restore_data(user_id, file_names):
    try:
        rdp = RestoreDataPipeline()
        rdp.user_id = user_id
        rdp.run_restore_data_pipeline(user_id, file_names)
        if rdp.last_processed_ts is None:
            logging.debug("After run, last_processed_ts == None, must be early return")
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

    def run_restore_data_pipeline(self, user_id, file_names):
        time_query = espq.get_time_range_for_restore_data(user_id)
        for file_name in file_names:
            entries_to_import = json.load(gzip.open(file_name + ".gz"), object_hook = esj.wrapped_object_hook)
            (tsdb_count, ucdb_count) = lmtfr.load_multi_timeline_for_range(file_prefix=file_name, continue_on_error=True, raw_timeseries_only=True)
            logging.debug("After load, tsdb_count = %s, ucdb_count = %s" % (tsdb_count, ucdb_count))
            if tsdb_count == 0:
                # Didn't process anything new so start at the same point next time
                self._last_processed_ts = None
            else:
                self._last_processed_ts = entries_to_import[-1]['data']['ts']
                logging.debug("After load, last_processed_ts = %s" % (self._last_processed_ts))
