import logging
import emission.analysis.configs.config_utils as eacc

def get_config(user_id):
    # right now, we are not doing any server side overrides, so we pick the
    # last user defined configuration for this user
    SENSOR_CONFIG_KEY = "config/sensor_config"
    return eacc.get_last_entry(user_id, SENSOR_CONFIG_KEY)
