# Standard imports
import logging
import random
import copy

# Our imports
import emission.storage.pipeline_queries as epq
import emission.storage.timeseries.abstract_timeseries as esta
import emission.storage.decorations.analysis_timeseries_queries as esda
import emission.core.wrapper.labelprediction as ecwl
import emission.core.wrapper.entry as ecwe

import emission.analysis.modelling.tour_model.load_predict as lp

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
def placeholder_predictor_2(trip):
    # Timestamp2index gives us a deterministic way to match test trips with labels
    # Hardcoded to match "test_july_22" -- clearly, this is just for testing
    timestamp2index = {494: 5, 565: 4, 795: 3, 805: 2, 880: 1, 960: 0}
    timestamp = trip["data"]["start_local_dt"]["hour"]*60+trip["data"]["start_local_dt"]["minute"]
    index = timestamp2index[timestamp] if timestamp in timestamp2index else 0
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
    ][index]


# This fourth scenario provides labels designed to test the expectation and notification system.
def placeholder_predictor_3(trip):
    timestamp2index = {494: 5, 565: 4, 795: 3, 805: 2, 880: 1, 960: 0}
    timestamp = trip["data"]["start_local_dt"]["hour"]*60+trip["data"]["start_local_dt"]["minute"]
    index = timestamp2index[timestamp] if timestamp in timestamp2index else 0
    return [
        [
            {"labels": {"mode_confirm": "bike", "purpose_confirm": "work"}, "p": 0.80},
            {"labels": {"mode_confirm": "walk", "purpose_confirm": "shopping"}, "p": 0.20}
        ],
        [
            {"labels": {"mode_confirm": "bike", "purpose_confirm": "work"}, "p": 0.80},
            {"labels": {"mode_confirm": "walk", "purpose_confirm": "shopping"}, "p": 0.20}
        ],
        [
            {"labels": {"mode_confirm": "drove_alone", "purpose_confirm": "entertainment"}, "p": 0.70},
        ],
        [
            {"labels": {"mode_confirm": "bike", "purpose_confirm": "work"}, "p": 0.96},
            {"labels": {"mode_confirm": "walk", "purpose_confirm": "shopping"}, "p": 0.04}
        ],
        [
            {"labels": {"mode_confirm": "walk", "purpose_confirm": "shopping"}, "p": 0.45},
            {"labels": {"mode_confirm": "walk", "purpose_confirm": "entertainment"}, "p": 0.35},
            {"labels": {"mode_confirm": "drove_alone", "purpose_confirm": "work"}, "p": 0.15},
            {"labels": {"mode_confirm": "shared_ride", "purpose_confirm": "work"}, "p": 0.05}
        ],
        [
            {"labels": {"mode_confirm": "walk", "purpose_confirm": "shopping"}, "p": 0.60},
            {"labels": {"mode_confirm": "walk", "purpose_confirm": "entertainment"}, "p": 0.25},
            {"labels": {"mode_confirm": "drove_alone", "purpose_confirm": "work"}, "p": 0.11},
            {"labels": {"mode_confirm": "shared_ride", "purpose_confirm": "work"}, "p": 0.04}
        ]
    ][index]

# Placeholder that is suitable for a demo.
# Finds all unique label combinations for this user and picks one randomly
def placeholder_predictor_demo(trip):
    import random

    import emission.core.get_database as edb
    user = trip["user_id"]
    unique_user_inputs = edb.get_analysis_timeseries_db().find({"user_id": user}).distinct("data.user_input")
    random_user_input = random.choice(unique_user_inputs) if random.randrange(0,10) > 0 else []

    logging.debug(f"In placeholder_predictor_demo: ound {len(unique_user_inputs)} for user {user}, returning value {random_user_input}")
    return [{"labels": random_user_input, "p": random.random()}]

# Non-placeholder implementation. First bins the trips, and then clusters every bin
# See emission.analysis.modelling.tour_model for more details
# Assumes that pre-built models are stored in working directory
# Models are built using evaluation_pipeline.py and build_save_model.py
def predict_two_stage_bin_cluster(trip):
    return lp.predict_labels(trip)

# For each algorithm in ecwl.AlgorithmTypes that runs on a trip (e.g., not the ensemble, which
# runs on the results of other algorithms), primary_algorithms specifies a corresponding
# function to run. This makes it easy to plug in additional algorithms later.
primary_algorithms = {
    ecwl.AlgorithmTypes.TWO_STAGE_BIN_CLUSTER: predict_two_stage_bin_cluster,
    ecwl.AlgorithmTypes.PLACEHOLDER_PREDICTOR_DEMO: placeholder_predictor_demo
}

# This placeholder ensemble simply returns the first prediction run
def ensemble_first_prediction(trip, predictions):
    # Since this is not a real ensemble yet, we will not mark it as such
    # algorithm_id = ecwl.AlgorithmTypes.ENSEMBLE
    algorithm_id = ecwl.AlgorithmTypes(predictions[0]["algorithm_id"]);
    prediction = copy.copy(predictions[0]["prediction"])
    return algorithm_id, prediction

# If we get a real prediction, use it, otherwise fallback to the placeholder
def ensemble_real_and_placeholder(trip, predictions):
        if predictions[0]["prediction"] != []:
            sel_prediction = predictions[0]
            logging.debug(f"Found real prediction {sel_prediction}, using that preferentially")
            # assert sel_prediction.algorithm_id == ecwl.AlgorithmTypes.TWO_STAGE_BIN_CLUSTER
        else:
            sel_prediction = predictions[1]
            logging.debug(f"No real prediction found, using placeholder prediction {sel_prediction}")
            # Use a not equal assert since we may want to change the placeholder
            assert sel_prediction.algorithm_id != ecwl.AlgorithmTypes.TWO_STAGE_BIN_CLUSTER

        algorithm_id = ecwl.AlgorithmTypes(sel_prediction["algorithm_id"])
        prediction = copy.copy(sel_prediction["prediction"])
        return algorithm_id, prediction

# ensemble specifies which algorithm of the several above to run.
# This makes it easy to test various ways of combining various algorithms.
ensemble = ensemble_first_prediction

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
            esda.CLEANED_TRIP_KEY, user_id, time_query=time_range)
        for cleaned_trip in self.toPredictTrips:
            # Create an inferred trip
            cleaned_trip_dict = copy.copy(cleaned_trip)["data"]
            inferred_trip = ecwe.Entry.create_entry(user_id, "analysis/inferred_trip", cleaned_trip_dict)
            
            # Run the algorithms and the ensemble, store results
            results = self.compute_and_save_algorithms(inferred_trip)
            ensemble = self.compute_and_save_ensemble(inferred_trip, results)

            # Put final results into the inferred trip and store it
            inferred_trip["data"]["cleaned_trip"] = cleaned_trip.get_id()
            inferred_trip["data"]["inferred_labels"] = ensemble["prediction"]
            self.ts.insert(inferred_trip)

            if self._last_trip_done is None or self._last_trip_done["data"]["end_ts"] < cleaned_trip["data"]["end_ts"]:
                self._last_trip_done = cleaned_trip
    
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
            lp.start_ts = trip["data"]["start_ts"]
            lp.end_ts = trip["data"]["end_ts"]
            self.ts.insert_data(self.user_id, "inference/labels", lp)
            predictions.append(lp)
        return predictions

    # Combine all our predictions into a single ensemble prediction.
    # As a placeholder, we just take the first prediction.
    # TODO: implement a real combination algorithm.
    def compute_and_save_ensemble(self, trip, predictions):
        il = ecwl.Labelprediction()
        il.trip_id = trip.get_id()
        il.start_ts = trip["data"]["start_ts"]
        il.end_ts = trip["data"]["end_ts"]
        (il.algorithm_id, il.prediction) = ensemble(trip, predictions)
        self.ts.insert_data(self.user_id, "analysis/inferred_labels", il)
        return il
