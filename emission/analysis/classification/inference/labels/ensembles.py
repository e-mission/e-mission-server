# This file encapsulates the various ensemble algorithms that take a trip and a list of primary predictions and return a label data structure

import copy
import logging

import emission.core.wrapper.labelprediction as ecwl

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