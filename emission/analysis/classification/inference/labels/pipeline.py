# Standard imports
import logging
import random
import copy

# Our imports
import emission.storage.pipeline_queries as epq
import emission.storage.timeseries.abstract_timeseries as esta
import emission.storage.decorations.analysis_timeseries_queries as esda
import emission.core.wrapper.labelprediction as ecwl

# Does all the work necessary for a given user
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

# A set of placeholder predictors to allow pipeline development without a real inference algorithm.
# For the moment, the system is configured to work with two labels, "mode_confirm" and
# "purpose_confirm", so I'll do that.

# The first placeholder scenario represents a case where it is hard to distinguish between
# biking and walking (e.g., because the user is a very slow biker) and hard to distinguish
# between work and shopping at the grocery store (e.g., because the user works at the
# grocery store), but whenever the user bikes to the location it is to work and whenever
# the user walks to the location it is to shop (e.g., because they don't have a basket on
# their bike), and the user bikes to the location four times more than they walk there.
# Obviously, it is a simplification.
def placeholder_predictor_0(trip):
    return [
        {"labels": {"mode_confirm": "bike", "purpose_confirm": "work"}, "p": 0.8},
        {"labels": {"mode_confirm": "walk", "purpose_confirm": "shopping"}, "p": 0.2}
    ]


# The next placeholder scenario provides that same set of labels in 75% of cases and no
# labels in the rest.
def placeholder_predictor_1(trip):
    return [
        {"labels": {"mode_confirm": "bike", "purpose_confirm": "work"}, "p": 0.8},
        {"labels": {"mode_confirm": "walk", "purpose_confirm": "shopping"}, "p": 0.2}
    ] if random.random() > 0.25 else []


# This third scenario provides labels designed to test the soundness and resilience of
# the client-side inference processing algorithms.
trip_n = 0  # ugly use of globals for testing only
def placeholder_predictor_2(trip):
    global trip_n  # ugly use of globals for testing only
    trip_n %= 6
    trip_n += 1
    return [
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

# For each algorithm in ecwl.AlgorithmTypes that runs on a trip (e.g., not the ensemble, which
# runs on the results of other algorithms), primary_algorithms specifies a corresponding
# function to run. This makes it easy to plug in additional algorithms later.
primary_algorithms = {
    # This can be edited to select a different placeholder predictor
    ecwl.AlgorithmTypes.PLACEHOLDER: placeholder_predictor_2
}

# Code structure based on emission.analysis.classification.inference.mode.pipeline
# and emission.analysis.classification.inference.mode.rule_engine
class LabelInferencePipeline:
    def __init__(self):
        self._last_trip_done = None
    
    @property
    def last_trip_done(self):
        return self._last_trip_done

    # For a given user and time range, runs all the primary algorithms and ensemble, saves results
    # to the database, and records progress
    def run_prediction_pipeline(self, user_id, time_range):
        self.ts = esta.TimeSeries.get_time_series(user_id)
        self.toPredictTrips = esda.get_entries(
            esda.CONFIRMED_TRIP_KEY, user_id, time_query=time_range)
        for trip in self.toPredictTrips:
            results = self.compute_and_save_algorithms(trip)
            ensemble = self.compute_and_save_ensemble(trip, results)

            # Add final prediction to the confirmed trip entry in the database
            trip["data"]["inferred_labels"] = ensemble["prediction"]
            self.ts.update(trip)
            if self._last_trip_done is None or self._last_trip_done.data.end_ts < trip.data.end_ts:
                self._last_trip_done = trip
    
    # This is where the labels for a given trip are actually predicted.
    # Though the only information passed in is the trip object, the trip object can provide the
    # user_id and other potentially useful information.
    def compute_and_save_algorithms(self, trip):
        predictions = []
        for algorithm_id, algorithm_fn in primary_algorithms.items():
            prediction = algorithm_fn(trip)
            lp = ecwl.Labelprediction()
            lp.trip_id = trip.get_id()
            lp.algorithm_id = algorithm_id
            lp.prediction = prediction
            lp.start_ts = trip.data.start_ts
            lp.end_ts = trip.data.end_ts
            self.ts.insert_data(self.user_id, "inference/labels", lp)
            predictions.append(lp)
        return predictions

    # Combine all our predictions into a single ensemble prediction.
    # As a placeholder, we just take the first prediction.
    # TODO: implement a real combination algorithm.
    def compute_and_save_ensemble(self, trip, predictions):
        il = ecwl.Labelprediction()
        il.trip_id = trip.get_id()
        # Since this is not a real ensemble yet, we will not mark it as such
        # il.algorithm_id = ecwl.AlgorithmTypes.ENSEMBLE
        il.algorithm_id = ecwl.AlgorithmTypes(predictions[0]["algorithm_id"])
        il.start_ts = trip.data.start_ts
        il.end_ts = trip.data.end_ts

        il.prediction = copy.copy(predictions[0]["prediction"])
        
        self.ts.insert_data(self.user_id, "analysis/inferred_labels", il)
        return il
