# Based on modeprediction.py
from emission.analysis.modelling.trip_model.model_type import ModelType
import emission.core.wrapper.wrapperbase as ecwb


class Tripmodel(ecwb.WrapperBase):
    props = {
        "model_type": ecwb.WrapperBase.Access.WORM,  # emission.analysis.modelling.trip_model.model_type.py
        "model":      ecwb.WrapperBase.Access.WORM,  # the (serialized) state of the model for this trip
        "model_ts":   ecwb.WrapperBase.Access.WORM,  # timestamp that model is "current" to wrt input data
    }

    enums = {
        "model_type": ModelType
    }
    geojson = {}
    local_dates = {}

    def _populateDependencies(self):
        pass
