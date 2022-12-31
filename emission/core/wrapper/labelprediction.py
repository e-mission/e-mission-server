# Based on modeprediction.py
import emission.core.wrapper.wrapperbase as ecwb
import enum

# The "prediction" data structure is a list of label possibilities, each one consisting of a set of labels and a probability:
# [
#    {"labels": {"labeltype1": "labelvalue1", "labeltype2": "labelvalue2"}, "p": 0.61},
#    {"labels": {"labeltype1": "labelvalue3", "labeltype2": "labelvalue4"}, "p": 0.27},
#    ...
# ]


class AlgorithmTypes(enum.Enum):
    ENSEMBLE = 0
    PLACEHOLDER_0 = 1
    PLACEHOLDER_1 = 2
    PLACEHOLDER_2 = 3
    PLACEHOLDER_3 = 4
    TWO_STAGE_BIN_CLUSTER = 5
    PLACEHOLDER_PREDICTOR_DEMO = 6
    CONFIDENCE_DISCOUNTED_CLUSTER = 7
    GRADIENT_BOOSTED_DECISION_TREE = 8


class Labelprediction(ecwb.WrapperBase):
    props = {"trip_id":     ecwb.WrapperBase.Access.WORM,  # the trip that this is part of
            "algorithm_id": ecwb.WrapperBase.Access.WORM,  # the algorithm that made this prediction
            "prediction":   ecwb.WrapperBase.Access.WORM,  # What we predict -- see above
            "start_ts":     ecwb.WrapperBase.Access.WORM,  # start time for the prediction, so that it can be captured in time-based queries, e.g. to reset the pipeline
            "end_ts":       ecwb.WrapperBase.Access.WORM,  # end time for the prediction, so that it can be captured in time-based queries, e.g. to reset the pipeline
    }

    enums = {
        "algorithm_id": AlgorithmTypes
    }
    geojson = {}
    local_dates = {}

    def _populateDependencies(self):
        pass
