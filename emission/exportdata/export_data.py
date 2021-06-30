# Standard imports 
import logging

# Our imports
import emission.storage.pipeline_queries as espq
import emission.storage.timeseries.abstract_timeseries as esta
import emission.storage.decorations.analysis_timeseries_queries as esda
import emission.storage.timeseries.cache_series as estcs
import emission.export.export as eee
import emission.core.wrapper.pipelinestate as ps
import gzip
import json
import bson.json_util as bju

def set_export_data(user_id):
    try:
        edp = ExportDataPipeline()
        edp.user_id = user_id
        edp.run_export_data_pipeline(user_id)
        if edp.last_trip_done is None:
            logging.debug("After run, last_trip_done == None, must be early return")
        espq.mark_export_data_done(user_id, edp.last_trip_done)
    except:
        logging.exception("Error while exporting, timestamp unchanged")
        espq.mark_export_data_failed(user_id)

class ExportDataPipeline:
    def __init__(self):
        self._last_trip_done = None

    @property
    def last_trip_done(self):
        return self._last_trip_done

    def run_export_data_pipeline(self, user_id):
        ts = esta.TimeSeries.get_time_series(user_id)
        time_query = espq.get_time_range_for_stage(user_id, ps.PipelineStages.EXPORT_DATA)
        loc_time_query = time_query
        loc_time_query.timeType = "data.ts"
        loc_entry_list = list(estcs.find_entries(user_id, key_list=None, time_query=loc_time_query))
        trip_time_query = time_query
        trip_time_query.timeType = "data.end_ts"
        trip_entry_list = list(ts.find_entries(key_list=None, time_query=trip_time_query))
        place_time_query = time_query
        place_time_query.timeType = "data.enter_ts"
        place_entry_list = list(ts.find_entries(key_list=None, time_query=place_time_query))
        ma_entry_list = []
        file_name = "export"
        eee.export(loc_entry_list, trip_entry_list, place_entry_list, ma_entry_list, user_id, file_name, ts)	        
