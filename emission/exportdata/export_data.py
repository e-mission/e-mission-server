# Standard imports 
import logging

# Our imports
import emission.storage.pipeline_queries as epq
import emission.storage.timeseries.abstract_timeseries as esta
import emission.storage.decorations.analysis_timeseries_queries as esda

import gzip
import json
import bson.json_util as bju

def set_export_data(user_id):
    time_query = epq.get_time_range_for_export_data(user_id)        # Does this have .startTs and .endTs atributes? Should we use those?
    try: 
        edp = ExportDataPipeline()
        edp.user_id = user_id
        edp.run_export_data_pipeline(user_id, time_query)
        if edp.last_trip_done is None:
            logging.debug("After run, last_trip_done == None, must be early return")
        epq.mark_export_data_done(user_id, edp.last_trip_done)
    except:
        logging.exception("Error while exporting, timestamp unchanged")
        epq.mark_export_data_failed(user_id)

class ExportDataPipeline:
    def __init__(self):
        self._last_trip_done = None

    @property
    def last_trip_done(self):
        return self._last_trip_done

    def run_export_data_pipeline(self, user_id, time_query):
        ts = esta.TimeSeries.get_time_series(user_id)
        
        #This will essentially give us a list of the raw data in return. There are no restrictions on what is added, all keys are added so long as they fall in the time range.
        raw_data_list = list(ts.find_entries(key_list=None, time_query=time_query))

        #This will give us just a list of the cleaned trips. (Another example option)
        #cleaned_trip_list = list(ts.find_entries(key_list = {'analysis/cleaned_trip'}, time_query=time_query))

        #File export
        export_file = "%s_%s.gz" % ("export", user_id)
        
        with gzip.open(export_file, "wt") as ef:
            json.dump(raw_data_list, ef, default=bju.default, allow_nan=False, indent=4)


         
