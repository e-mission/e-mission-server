import json
import re
from this import d
from typing import Optional
import logging
from numpy import isin

import emission.analysis.modelling.trip_model.model_storage as eamums
import emission.analysis.modelling.trip_model.model_type as eamumt

config_filename = ""

def load_config():
    global config_filename
    try:
        config_filename = 'conf/analysis/trip_model.conf.json'
        config_file = open(config_filename)
    except:
        print("analysis.trip_model.conf.json not configured, falling back to sample, default configuration")
        config_filename = 'conf/analysis/trip_model.conf.json.sample'
        config_file = open('conf/analysis/trip_model.conf.json.sample')
    ret_val = json.load(config_file)
    config_file.close()
    return ret_val

config_data = load_config()

def reload_config():
    global config_data
    config_data = load_config()

def get_config():
    return config_data

def get_optional_config_value(key) -> Optional[str]:
    """
    get a config value at the provided path/key

    :param key: a key name or a dot-delimited path to some key within the config object
    :return: the value at the key, or, None if not found
    """
    cursor = config_data
    path = key.split(".")
    for k in path:
        cursor = cursor.get(k)
        if cursor is None:
            return None
    return cursor

def get_config_value_or_raise(key):
    logging.debug(f'getting key {key} in config')
    value = get_optional_config_value(key)
    if value is None:
        logging.debug('config object:')
        logging.debug(json.dumps(config_data, indent=2))
        msg = f"expected config key {key} not found in config file {config_filename}"
        raise KeyError(msg)
    else:
        return value

def get_model_type():
    model_type_str = get_config_value_or_raise('model_type')
    model_type = eamumt.ModelType.from_str(model_type_str)
    return model_type

def get_model_storage():
    model_storage_str = get_config_value_or_raise('model_storage')
    model_storage = eamums.ModelStorage.from_str(model_storage_str)
    return model_storage

def get_minimum_trips():
    minimum_trips = get_config_value_or_raise('minimum_trips')
    if not isinstance(minimum_trips, int):
        msg = f"config key 'minimum_trips' not an integer in config file {config_filename}"
        raise TypeError(msg)
    return minimum_trips



