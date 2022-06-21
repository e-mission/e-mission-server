# Based on modeprediction.py
from emission.analysis.modelling.user_label_model.model_type import ModelType
import emission.core.wrapper.wrapperbase as ecwb


class UserLabelPredictionModel(ecwb.WrapperBase):
    props = {"user_id":   ecwb.WrapperBase.Access.WORM,  # the trip that this is part of
            "model_type": ecwb.WrapperBase.Access.WORM,  # emission.analysis.modelling.user_label_model.model_type.py
            "model":      ecwb.WrapperBase.Access.WORM,  # the (serialized) state of the model for this trip
            "model_ts":   ecwb.WrapperBase.Access.WORM,  # time that this model was stored
    }

    enums = {
        "model_type": ModelType
    }
    geojson = {}
    local_dates = {}

    def _populateDependencies(self):
        pass
