# Standard imports
import logging

# Our imports
import emission.storage.pipeline_queries as epq
import emission.storage.timeseries.abstract_timeseries as esta
import emission.storage.decorations.analysis_timeseries_queries as esda
import emission.core.wrapper.labelprediction as ecwl

def infer_labels(user_id):
    time_query = epq.get_time_range_for_label_inference(user_id)
    try:
        lip = LabelInferencePipeline()
        lip.user_id = user_id
        lip.run_prediction_pipeline(user_id, time_query)
        if lip.last_trip_done is None:
            logging.debug("After run, last_trip_done == None, must be early return")
        epq.mark_label_inference_done(user_id, lip.last_trip_done)
    except:
        logging.exception("Error while inferring labels, timestamp is unchanged")
        epq.mark_label_inference_failed(user_id)

# Code structure based on emission.analysis.classification.inference.mode.pipeline
# and emission.analysis.classification.inference.mode.rule_engine
class LabelInferencePipeline:
    def __init__(self):
        self._last_trip_done = None
    
    @property
    def last_trip_done(self):
        return self._last_trip_done

    def run_prediction_pipeline(self, user_id, time_range):
        self.ts = esta.TimeSeries.get_time_series(user_id)
        self.toPredictTrips = esda.get_entries(
            esda.CLEANED_TRIP_KEY, user_id, time_query=time_range)
        for trip in self.toPredictTrips:
            prediction = predict_trip(trip)
            lp = ecwl.Labelprediction()
            lp.trip_id = trip.get_id()
            lp.prediction = prediction
            lp.start_ts = trip.data.start_ts
            lp.end_ts = trip.data.end_ts
            # Insert the Labelprediction into the database as its own independent document
            self.ts.insert_data(self.user_id, esda.INFERRED_LABELS_KEY, lp)
            if self._last_trip_done is None or self._last_trip_done.data.end_ts < trip.data.end_ts:
                self._last_trip_done = trip

# This is where the labels for a given trip are actually predicted.
# Though the only information passed in is the trip object, the trip object can provide the
# user_id and other potentially useful information.
def predict_trip(trip):
    return placeholder_prediction(trip)

# For testing only!
trip_n = 0
import random

# A placeholder predictor to allow pipeline development without a real inference algorithm
def placeholder_prediction(trip, scenario=2):
    # For the moment, the system is configured to work with two labels, "mode_confirm" and
    # "purpose_confirm", so I'll do that.

    # For testing only!
    global trip_n
    trip_n %= 6
    trip_n += 1

    return [
        # The first placeholder scenario represents a case where it is hard to distinguish between
        # biking and walking (e.g., because the user is a very slow biker) and hard to distinguish
        # between work and shopping at the grocery store (e.g., because the user works at the
        # grocery store), but whenever the user bikes to the location it is to work and whenever
        # the user walks to the location it is to shop (e.g., because they don't have a basket on
        # their bike), and the user bikes to the location four times more than they walk there.
        # Obviously, it is a simplification.
        [
            {"labels": {"mode_confirm": "bike", "purpose_confirm": "work"}, "p": 0.8},
            {"labels": {"mode_confirm": "walk", "purpose_confirm": "shopping"}, "p": 0.2}
        ],

        # The next placeholder scenario provides that same set of labels in 75% of cases and no
        # labels in the rest.
        [
            {"labels": {"mode_confirm": "bike", "purpose_confirm": "work"}, "p": 0.8},
            {"labels": {"mode_confirm": "walk", "purpose_confirm": "shopping"}, "p": 0.2}
        ]
        if random.random() > 0.25 else [],

        # This third scenario provides labels designed to test the soundness and resilience of
        # the client-side inference processing algorithms.
        [
            [

            ],
            [
                {"labels": {"mode_confirm": "bike", "purpose_confirm": "work"}, "p": 0.8},
                {"labels": {"mode_confirm": "walk", "purpose_confirm": "shopping"}, "p": 0.2}
            ],
            [
                {"labels": {"mode_confirm": "drove_alone"}, "p": 0.8},
            ],
            [
                {"labels": {"mode_confirm": "bike", "purpose_confirm": "work"}, "p": 0.8},
                {"labels": {"mode_confirm": "walk", "purpose_confirm": "shopping"}, "p": 0.2}
            ],
            [
                {"labels": {"mode_confirm": "walk", "purpose_confirm": "shopping"}, "p": 0.45},
                {"labels": {"mode_confirm": "walk", "purpose_confirm": "entertainment"}, "p": 0.35},
                {"labels": {"mode_confirm": "drove_alone", "purpose_confirm": "work"}, "p": 0.15},
                {"labels": {"mode_confirm": "shared_ride", "purpose_confirm": "work"}, "p": 0.05}
            ],
            [
                {"labels": {"mode_confirm": "walk", "purpose_confirm": "shopping"}, "p": 0.45},
                {"labels": {"mode_confirm": "walk", "purpose_confirm": "entertainment"}, "p": 0.35},
                {"labels": {"mode_confirm": "drove_alone", "purpose_confirm": "work"}, "p": 0.15},
                {"labels": {"mode_confirm": "shared_ride", "purpose_confirm": "work"}, "p": 0.05}
            ]
        ][6-trip_n]
    ][scenario]
    
