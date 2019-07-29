import logging
import emission.core.wrapper.wrapperbase as ecwb
import emission.core.wrapper.motionactivity as ecwm
import enum as enum


class AlgorithmTypes(enum.Enum):
    '''
    We could use NOT_PREDICTED to indicate a prediction that is empty. If a prediction returns zero,
        it can be incorrectly assumed that the mode predicted it IN_VEHICLE if the prediction returned an error or was never
        made in the first place. Using NOT_PREDICTED can help distinguish the true zeros from the erroneous ones.

    '''
    SEED_RANDOM_FOREST = 1
    SIMPLE_RULE_ENGINE = 2

class PredictedModeTypes(enum.Enum):
    UNKNOWN = 0
    WALKING = 1
    BICYCLING = 2
    BUS = 3
    TRAIN = 4
    CAR = 5
    AIR_OR_HSR = 6
    SUBWAY = 7
    TRAM = 8

class Modeprediction(ecwb.WrapperBase):
    props = {"trip_id":     ecwb.WrapperBase.Access.WORM,     # the trip that this is part of
            "section_id":     ecwb.WrapperBase.Access.WORM,     # The section id that this prediction corresponds to
            "algorithm_id": ecwb.WrapperBase.Access.WORM,     # The algorithm which made this prediction
            "sensed_mode":     ecwb.WrapperBase.Access.WORM,     # The mode that the phones sensors picked up
            "predicted_mode_map": ecwb.WrapperBase.Access.WORM,   # What we predicted
            "start_ts": ecwb.WrapperBase.Access.WORM, # start time for the prediction, so that it can be captured in time-based queries, e.g. to reset the pipeline
            "end_ts": ecwb.WrapperBase.Access.WORM, # end time for the prediction, so that it can be captured in time-based queries, e.g. to reset the pipeline
    }

    enums = {
        "sensed_mode": ecwm.MotionTypes,
        "algorithm_id": AlgorithmTypes
    }

    geojson = {}
    local_dates = {}

    def _populateDependencies(self):
        pass
