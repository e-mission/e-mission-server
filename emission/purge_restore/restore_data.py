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

def restore_data(user_id, file_name):
    try:
        rdp = RestoreDataPipeline()
        rdp.user_id = user_id
        rdp.run_restore_data_pipeline(user_id, file_name)
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

    def run_restore_data_pipeline(self, user_id, file_name):
        time_query = espq.get_time_range_for_restore_data(user_id)
        entries = json.load(gzip.open(file_name + ".gz"), object_hook = esj.wrapped_object_hook)
        '''
        PipelineState({
            '_id': ObjectId('66b15dd496328b58cca9486d'), 
            'user_id': UUID('6f600819-2b42-47a8-a57c-c7db631a832a'), 
            'pipeline_stage': 20, 
            'curr_run_ts': None, 
            'last_processed_ts': 1437633640.069, 
            'last_ts_run': 1722899919.6985111
        })
        '''
        # pipelineState = edb.get_pipeline_state_db().find_one({"user_id": user_id,
        #     "pipeline_stage": ecwp.PipelineStages.RESTORE_TIMESERIES_DATA.value})
        # self._last_processed_ts = pipelineState["last_processed_ts"]
        # logging.debug("Restoring from file, last_processed_ts = %s" % (self._last_processed_ts))
        (tsdb_count, ucdb_count) = lmtfr.load_multi_timeline_for_range(file_prefix=file_name, continue_on_error=True)
        print("After load, tsdb_count = %s, ucdb_count = %s" % (tsdb_count, ucdb_count))
        if tsdb_count == 0:
            # Didn't process anything new so start at the same point next time
            self._last_processed_ts = None
        else:
            ts_values = [entry['data']['ts'] for entry in entries]
            self._last_processed_ts = max(ts_values)
            print("After load, last_processed_ts = %s" % (self._last_processed_ts))
        # if self._last_processed_ts is None or self._last_processed_ts < entries[-1]['metadata']['write_ts']:
        #     self._last_processed_ts = entries[-1]['metadata']['write_ts']
