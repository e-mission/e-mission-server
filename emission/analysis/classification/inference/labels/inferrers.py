# This file encapsulates the various prediction algorithms that take a trip and return a label data structure
# Named "inferrers.py" instead of "predictors.py" to avoid a name collection in our abbreviated import convention

import logging
import random
import copy
import time
import arrow

import emission.analysis.modelling.tour_model_first_only.load_predict as lp
import emission.analysis.modelling.trip_model.run_model as eamur
import emission.analysis.modelling.trip_model.config as eamtc

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
def placeholder_predictor_0(trip_list):
    predictions_list = []
    for trip in trip_list:
        predictions = [
            {"labels": {"mode_confirm": "bike", "purpose_confirm": "work"}, "p": 0.8},
            {"labels": {"mode_confirm": "walk", "purpose_confirm": "shopping"}, "p": 0.2}
        ]
        predictions_list.append(predictions)
    return predictions_list


# The next placeholder scenario provides that same set of labels in 75% of cases and no
# labels in the rest.
def placeholder_predictor_1(trip_list):
    predictions_list = []
    for trip in trip_list:
        predictions = [
            {"labels": {"mode_confirm": "bike", "purpose_confirm": "work"}, "p": 0.8},
            {"labels": {"mode_confirm": "walk", "purpose_confirm": "shopping"}, "p": 0.2}
        ] if random.random() > 0.25 else []
        predictions_list.append(predictions)
    return predictions_list



# This third scenario provides labels designed to test the soundness and resilience of
# the client-side inference processing algorithms.
def placeholder_predictor_2(trip_list):
    predictions_list = []
    for trip in trip_list:
        # Timestamp2index gives us a deterministic way to match test trips with labels
        # Hardcoded to match "test_july_22" -- clearly, this is just for testing
        timestamp2index = {494: 5, 565: 4, 795: 3, 805: 2, 880: 1, 960: 0}
        timestamp = trip["data"]["start_local_dt"]["hour"]*60+trip["data"]["start_local_dt"]["minute"]
        index = timestamp2index[timestamp] if timestamp in timestamp2index else 0
        predictions = [
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
        predictions_list.append(predictions)
    return predictions_list


# This fourth scenario provides labels designed to test the expectation and notification system.
def placeholder_predictor_3(trip_list):
    predictions_list = []
    for trip in trip_list:
        timestamp2index = {494: 5, 565: 4, 795: 3, 805: 2, 880: 1, 960: 0}
        timestamp = trip["data"]["start_local_dt"]["hour"]*60+trip["data"]["start_local_dt"]["minute"]
        index = timestamp2index[timestamp] if timestamp in timestamp2index else 0
        predictions = [
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
        predictions_list.append(predictions)
    return predictions_list

# Placeholder that is suitable for a demo.
# Finds all unique label combinations for this user and picks one randomly
def placeholder_predictor_demo(trip_list):
    import random
    import emission.core.get_database as edb
    
    unique_user_inputs = edb.get_analysis_timeseries_db().find({"user_id": user_id}).distinct("data.user_input")
    predictions_list = []
    if len(unique_user_inputs) == 0:
        predictions_list.append([])
        return predictions_list
    random_user_input = random.choice(unique_user_inputs) if random.randrange(0,10) > 0 else []

    logging.debug(f"In placeholder_predictor_demo: found {len(unique_user_inputs)} for user {user_id}, returning value {random_user_input}")
    predictions_list.append([{"labels": random_user_input, "p": random.random()}])
    return predictions_list

# Non-placeholder implementation. First bins the trips, and then clusters every bin
# See emission.analysis.modelling.tour_model for more details
# Assumes that pre-built models are stored in working directory
# Models are built using evaluation_pipeline.py and build_save_model.py
# This algorithm is now DEPRECATED in favor of predict_cluster_confidence_discounting (see https://github.com/e-mission/e-mission-docs/issues/663)
def predict_two_stage_bin_cluster(trip):
    return lp.predict_labels(trip)

# Reduce the confidence of the clustering prediction when the number of trips in the cluster is small
# See https://github.com/e-mission/e-mission-docs/issues/663
def n_to_confidence_coeff(n, max_confidence=None, first_confidence=None, confidence_multiplier=None):
    if max_confidence is None: max_confidence = 0.99  # Confidence coefficient for n approaching infinity -- in the GitHub issue, this is 1-A
    if first_confidence is None: first_confidence = 0.80  # Confidence coefficient for n = 1 -- in the issue, this is B
    if confidence_multiplier is None: confidence_multiplier = 0.30  # How much of the remaining removable confidence to remove between n = k and n = k+1 -- in the issue, this is C
    return max_confidence-(max_confidence-first_confidence)*(1-confidence_multiplier)**(n-1)  # This is the u = ... formula in the issue

# predict_two_stage_bin_cluster but with the above reduction in confidence
def predict_cluster_confidence_discounting(trip_list, max_confidence=None, first_confidence=None, confidence_multiplier=None):
    # load application config 
    model_type = eamtc.get_model_type()
    model_storage = eamtc.get_model_storage()

    # assert and fetch unique user id for trip_list
    user_id_list = []
    for trip in trip_list:
        user_id_list.append(trip['user_id'])
    assert user_id_list.count(user_id_list[0]) == len(user_id_list), "Multiple user_ids found for trip_list, expected unique user_id for all trips"
    # Assertion successful, use unique user_id
    user_id = user_id_list[0]

    # load model
    start_model_load_time = time.process_time()
    model = eamur._load_stored_trip_model(user_id, model_type, model_storage)
    print(f"{arrow.now()} Inside predict_labels_n: Model load time = {time.process_time() - start_model_load_time}")

    labels_n_list = eamur.predict_labels_with_n(trip_list, model)
    predictions_list = []
    for labels, n in labels_n_list:
        if n <= 0:  # No model data or trip didn't match a cluster
            logging.debug(f"In predict_cluster_confidence_discounting: n={n}; returning as-is")
            predictions_list.append(labels)
            continue
        confidence_coeff = n_to_confidence_coeff(n, max_confidence, first_confidence, confidence_multiplier)
        logging.debug(f"In predict_cluster_confidence_discounting: n={n}; discounting with coefficient {confidence_coeff}")
        labels = copy.deepcopy(labels)
        for l in labels: l["p"] *= confidence_coeff
        predictions_list.append(labels)
    return predictions_list
