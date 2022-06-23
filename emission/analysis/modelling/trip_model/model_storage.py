from enum import Enum
from typing import Dict, Optional

import emission.analysis.modelling.trip_model.model_type as eamum
import emission.core.wrapper.tripmodel as ecwu
import emission.storage.decorations.analysis_timeseries_queries as esda
import emission.storage.pipeline_queries as epq
import emission.storage.timeseries.abstract_timeseries as esta
import emission.storage.timeseries.builtin_timeseries as estb
import pymongo


class ModelStorage(Enum):
    """
    enumeration of model storage destinations. currently restricted to 
    DATABASE only.
    """
    DATABASE = 0
    @classmethod
    def names(cls):
        return list(map(lambda e: e.name, list(cls)))


def load_model(user_id, model_type: eamum.ModelType, model_storage: ModelStorage) -> Optional[Dict]:
    """load a user label model from a model storage location

    :param user_id: the user to request a model for
    :param model_type: expected type of model stored
    :param model_storage: storage format 
    :return: the model representation as a Python Dict or None
    :raises: TypeError if loaded model has different type than expected type
    """
    if model_storage == ModelStorage.DATABASE:
        
        # retrieve stored model with timestamp that matches/exceeds the most
        # recent PipelineState.TRIP_MODEL entry        
        ts = esda.get_timeseries_for_user(user_id)
        if not isinstance(ts, estb.BuiltinTimeSeries):
            raise Exception('user model storage requires BuiltInTimeSeries')
        latest_model_entry = ts.get_first_entry(
            key=esda.TRIP_MODEL_STORE_KEY,
            field='data.model_ts',
            sort_order=pymongo.DESCENDING
        )
        if latest_model_entry.model_type != model_type:
            msg = (
                f"loading model for user {user_id} has model type {latest_model_entry.model_type} " 
                f"but was expected to have model type {model_type}"
            )
            raise TypeError(msg)
        model = latest_model_entry.model if latest_model_entry is not None else None
        return model

    else:
        storage_types_str = ",".join(ModelStorage.names())
        msg = (
            f"unknown model storage type {model_storage}, must be one of "
            f"{{{storage_types_str}}}"
        )
        raise TypeError(msg)

def save_model(
    user_id, 
    model_type: eamum.ModelType, 
    model_data: Dict,
    model_timestamp: int,
    model_storage: ModelStorage = ModelStorage.DATABASE):
    """saves a model to storage

    :param user_id: user associated with this model
    :param model_type: type of model stored
    :param model_data: data for this model to store, should be a dict
    :param model_storage: type of storage to load from, defaults to ModelStorage.DATABASE
    :raises TypeError: unknown ModelType
    :raises IOError: failure when writing to storage medium
    """
   
    if model_storage == ModelStorage.DATABASE:
        
        row = ecwu.Tripmodel()
        row.user_id = user_id
        row.model_ts = model_timestamp
        row.model_type = model_type
        row.model = model_data

        try:
            ts = esta.TimeSeries.get_time_series(user_id)
            ts.insert_data(user_id, esda.TRIP_MODEL_STORE_KEY, row)
        except Exception as e:
            msg = (
                f"failure storing model for user {user_id}, model {model_type.name} "
                f"to the database"
            )
            raise IOError(msg) from e

        try:
            epq.mark_trip_model_done(user_id, model_timestamp)
        except Exception as e:
            msg = (
                f"failure updating user label pipeline state for user {user_id}"
            )
            raise IOError(msg) from e
    
    else:
            storage_types_str = ",".join(ModelStorage.names())
            msg = (
                f"unknown model storage type {model_storage}, must be one of "
                f"{{{storage_types_str}}}"
            )
            raise TypeError(msg)
