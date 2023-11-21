# Standard imports
import logging
import random
import copy
import time
import arrow

# Our imports
import emission.storage.pipeline_queries as epq
import emission.storage.timeseries.abstract_timeseries as esta
import emission.storage.decorations.analysis_timeseries_queries as esda
import emission.core.wrapper.labelprediction as ecwl
import emission.core.wrapper.entry as ecwe
import emission.analysis.classification.inference.labels.inferrers as eacili
import emission.analysis.classification.inference.labels.ensembles as eacile


# For each algorithm in ecwl.AlgorithmTypes that runs on a trip (e.g., not the ensemble, which
# runs on the results of other algorithms), primary_algorithms specifies a corresponding
# function in eacili to run. This makes it easy to plug in additional algorithms later.
primary_algorithms = {
    ecwl.AlgorithmTypes.CONFIDENCE_DISCOUNTED_CLUSTER: eacili.predict_cluster_confidence_discounting
}

# ensemble specifies which algorithm in eacile to run.
# This makes it easy to test various ways of combining various algorithms.
ensemble = eacile.ensemble_first_prediction


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
        
        cleaned_trip_list = self.toPredictTrips
        inferred_trip_list = []
        results_dict = {}
        ensemble_list = []

        # Create list of inferred trips
        for cleaned_trip in cleaned_trip_list:
            cleaned_trip_dict = copy.copy(cleaned_trip)["data"]
            inferred_trip = ecwe.Entry.create_entry(user_id, "analysis/inferred_trip", cleaned_trip_dict)
            inferred_trip_list.append(inferred_trip)
    
        # Computing outside loop by passing trip_list to ensure model loads once
        # Run the algorithms and the ensemble, store results
        results_dict = self.compute_and_save_algorithms(user_id, inferred_trip_list)
        ensemble_list = self.compute_and_save_ensemble(inferred_trip_list, results_dict)

        start_insert_inferred_trip_time = time.process_time()
        for cleaned_trip, inferred_trip, ensemble in zip(cleaned_trip_list, inferred_trip_list, ensemble_list):
            # Put final results into the inferred trip and store it
            inferred_trip["data"]["cleaned_trip"] = cleaned_trip.get_id()
            inferred_trip["data"]["inferred_labels"] = ensemble["prediction"]
            # start_0 = time.process_time()
            self.ts.insert(inferred_trip)
            # print(f"Inside run_prediction_pipeline: inferred_trip insert time = {time.process_time() - start_0}")

            if self._last_trip_done is None or self._last_trip_done["data"]["end_ts"] < cleaned_trip["data"]["end_ts"]:
                self._last_trip_done = cleaned_trip
        print(f"{arrow.now()} Inside run_prediction_pipeline: Saving inferred_trip total time = {time.process_time() - start_insert_inferred_trip_time}")
    
    # This is where the labels for a given trip are actually predicted.
    # Though the only information passed in is the trip object, the trip object can provide the
    # user_id and other potentially useful information.
    def compute_and_save_algorithms(self, user_id, trip_list):
        predictions_dict = {trip.get_id(): [] for trip in trip_list}
        for algorithm_id, algorithm_fn in primary_algorithms.items():
            prediction_list = algorithm_fn(user_id, trip_list)
            start_insert_inference_labels_time = time.process_time()
            for trip, prediction in zip(trip_list, prediction_list):
                lp = ecwl.Labelprediction()
                lp.algorithm_id = algorithm_id
                lp.trip_id = trip.get_id()
                lp.prediction = prediction
                lp.start_ts = trip["data"]["start_ts"]
                lp.end_ts = trip["data"]["end_ts"]
                # start_1 = time.process_time()
                self.ts.insert_data(self.user_id, "inference/labels", lp)
                # print(f"Inside compute_and_save_algorithms: inference/labels insert time = {time.process_time() - start_1}")
                predictions_dict[trip.get_id()].append(lp)
            print(f"{arrow.now()} Inside compute_and_save_algorithms: Saving inference/labels total time = {time.process_time() - start_insert_inference_labels_time}")
        return predictions_dict

    # Combine all our predictions into a single ensemble prediction.
    # As a placeholder, we just take the first prediction.
    # TODO: implement a real combination algorithm.
    def compute_and_save_ensemble(self, trip_list, predictions_dict):
        start_insert_inferred_labels_time = time.process_time()
        il_list = []
        for trip, key in zip(trip_list, predictions_dict):
            il = ecwl.Labelprediction()
            il.trip_id = trip.get_id()
            il.start_ts = trip["data"]["start_ts"]
            il.end_ts = trip["data"]["end_ts"]
            (il.algorithm_id, il.prediction) = ensemble(trip, predictions_dict[key])
            # start_2 = time.process_time()
            self.ts.insert_data(self.user_id, "analysis/inferred_labels", il)
            # print(f"Inside compute_and_save_ensemble: inferred_labels insert time = {time.process_time() - start_2}")
            il_list.append(il)
        print(f"{arrow.now()} Inside compute_and_save_ensemble: Saving inferred_labels total time = {time.process_time() - start_insert_inferred_labels_time}")
        return il_list
