# This file encapsulates the various prediction algorithms that take a trip and return a label data structure
# Named "inferrers.py" instead of "predictors.py" to avoid a name collection in our abbreviated import convention

import logging
import random
import copy

import emission.analysis.modelling.tour_model_first_only.load_predict as lp

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
    if len(unique_user_inputs) == 0:
        return []
    random_user_input = random.choice(unique_user_inputs) if random.randrange(0,10) > 0 else []

    logging.debug(f"In placeholder_predictor_demo: found {len(unique_user_inputs)} for user {user}, returning value {random_user_input}")
    return [{"labels": random_user_input, "p": random.random()}]

# Non-placeholder implementation. First bins the trips, and then clusters every bin
# See emission.analysis.modelling.tour_model for more details
# Assumes that pre-built models are stored in working directory
# Models are built using evaluation_pipeline.py and build_save_model.py
# This algorithm is now DEPRECATED in favor of predict_cluster_confidence_discounting (see https://github.com/e-mission/e-mission-docs/issues/663)
def predict_two_stage_bin_cluster(trip):
    return lp.predict_labels(trip)

# Reduce the confidence of the clustering prediction when the number of trips in the cluster is small
# See https://github.com/e-mission/e-mission-docs/issues/663
def n_to_confidence_coeff(n):
    MAX_CONFIDENCE = 0.99  # Confidence coefficient for n approaching infinity -- in the GitHub issue, this is 1-A
    FIRST_CONFIDENCE = 0.80  # Confidence coefficient for n = 1 -- in the issue, this is B
    CONFIDENCE_MULTIPLIER = 0.30  # How much of the remaining removable confidence to remove between n = k and n = k+1 -- in the issue, this is C
    return MAX_CONFIDENCE-(MAX_CONFIDENCE-FIRST_CONFIDENCE)*(1-CONFIDENCE_MULTIPLIER)**(n-1)  # This is the u = ... formula in the issue

# predict_two_stage_bin_cluster but with the above reduction in confidence
def predict_cluster_confidence_discounting(trip):
    labels, n = lp.predict_labels_with_n(trip)
    if n <= 0:  # No model data or trip didn't match a cluster
        logging.debug(f"In predict_cluster_confidence_discounting: n={n}; returning as-is")
        return labels

    confidence_coeff = n_to_confidence_coeff(n)
    logging.debug(f"In predict_cluster_confidence_discounting: n={n}; discounting with coefficient {confidence_coeff}")

    labels = copy.deepcopy(labels)
    for l in labels: l["p"] *= confidence_coeff
    return labels
