from enum import Enum
from typing import Dict, Optional
import logging
import json

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
    DOCUMENT_DATABASE only.
    """
    DOCUMENT_DATABASE = 0
    @classmethod
    def names(cls):
        return list(map(lambda e: e.name, list(cls)))

    @classmethod
    def from_str(cls, str):
        """
        attempts to match the provided string to a known ModelStorage type.
        not case sensitive.
        
        :param str: a string name of a ModelType
        """
        try:
            str_caps = str.upper()
            return cls[str_caps]
        except KeyError:
            names = "{" + ",".join(cls.names) + "}"
            msg = f"{str} is not a known ModelStorage, must be one of {names}"
            raise KeyError(msg)

def load_model(user_id, model_type: eamum.ModelType, model_storage: ModelStorage) -> Optional[Dict]:
    """load a user label model from a model storage location

    :param user_id: the user to request a model for
    :param model_type: expected type of model stored
    :param model_storage: storage format 
    :return: the model representation as a Python Dict or None
    :raises: TypeError if loaded model has different type than expected type
             KeyError if the ModelType is not known
    """
    if model_storage == ModelStorage.DOCUMENT_DATABASE:
        
        # retrieve stored model with timestamp that matches/exceeds the most
        # recent PipelineState.TRIP_MODEL entry        
        ts = esda.get_timeseries_for_user(user_id)
        if not isinstance(ts, estb.BuiltinTimeSeries):
            raise Exception('user model storage requires BuiltInTimeSeries')
        latest_model_entry = ts.get_first_entry(
            key=esda.TRIP_MODEL_STORE_KEY,
            field='metadata.write_ts',
            sort_order=pymongo.DESCENDING
        )

        if latest_model_entry is None:
            logging.debug(f'no {model_type.name} model found for user {user_id}')
            return None

        write_ts = latest_model_entry['metadata']['write_ts']
        logging.debug(f'retrieved latest trip model recorded at timestamp {write_ts}')
        logging.debug(latest_model_entry)

        # parse str to enum for ModelType
        latest_model_type_str = latest_model_entry.get('data', {}).get('model_type')
        if latest_model_type_str is None:
            raise TypeError('stored model does not have a model type')
        latest_model_type = eamum.ModelType.from_str(latest_model_type_str)
        
        # validate and return
        if latest_model_entry is None:
            return None
        elif latest_model_type != model_type:
            msg = (
                f"loading model for user {user_id} has model type '{latest_model_type.name}' " 
                f"but was expected to have model type {model_type.name}"
            )
            raise TypeError(msg)
        else:
            return latest_model_entry['data']['model']

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
    model_storage: ModelStorage = ModelStorage.DOCUMENT_DATABASE):
    """saves a model to storage

    :param user_id: user associated with this model
    :param model_type: type of model stored
    :param model_data: data for this model to store, should be a dict
    :param model_timestamp: time that model is current to
    :param model_storage: type of storage to load from, defaults to ModelStorage.DATABASE
    :raises TypeError: unknown ModelType
    :raises IOError: failure when writing to storage medium
    """
    if len(model_data) == 0:
        # this wouldn't be good, esp for incremental models, because it can 
        # wipe out all of a model's history. save_model should be avoided at the
        # call site when the model is empty.
        msg = f'trip model for user {user_id} is empty but save_model called'
        raise Exception(msg)

    if model_storage == ModelStorage.DOCUMENT_DATABASE:
        
        row = ecwu.Tripmodel()
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
    
    else:
            storage_types_str = ",".join(ModelStorage.names())
            msg = (
                f"unknown model storage type {model_storage}, must be one of "
                f"{{{storage_types_str}}}"
            )
            raise TypeError(msg)
