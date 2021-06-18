# Standard imports 
import logging

# Our imports
import emission.storage.pipeline_queries as epq
import emission.storage.timeseries.abstract_timeseries as esta
import emission.storage.decorations.analysis_timeseries_queries as esda
import emission.core.wrapper.itinerum_boolean as ecwib


def set_itinerum_boolean(user_id):
    time_query = epq.get_time_range_for_itinerum_boolean(user_id)
    try: 
        ibp = ItinerumBooleanPipeline()
        ibp.user_id = user_id
        ibp.run_itinerum_boolean_pipeline(user_id, time_query)
        if ibp.last_trip_done is None:
            logging.debug("After run, last_trip_done == None, must be early return")
        epq.mark_itinerum_boolean_done(user_id, ibp.last_trip_done)
    except:
        logging.exception("Error while setting boolean, timestamp unchanged")
        epq.mark_itinerum_boolean_failed(user_id)

class ItinerumBooleanPipeline:
    def __init__(self):
        self.last_trip_done = None

    @property
    def last_trip_done(self):
        return self._last_trip_done

    def run_itinerum_boolean_pipeline(self, user_id, time_range):
        #This is where we need to set the boolean?? 
        #How is this done? 
        #1.) seems to need to set the time series 
        self.ts = esta.TimeSeries.get_time_series(user_id)
        self.toSetFalseTrips = esda.get_entries(
                esda.ITINERUM_BOOLEAN_KEY, user_id, time_query=time_range)
        for trip in self.toSetFalseTrips:
            itinerum_boolean = False
            ib = ecwib.Itinerum_Boolean()
            ib.trip_id = trip.get_id()
            ib.itinerum_received = itinerum_boolean
            ib.start_ts = trip.data.start_ts
            ib.end_ts = trip.data.end_ts
            #Insert into the database as the boolean "false"
            self.ts.insert_data(self.user_id, esda.ITINERUM_BOOLEAN, ib)
            if self._last_trip_done is None or self._last_trip_done.data.end_ts < trip.data.end_ts:
                self._last_trip_done = trip
