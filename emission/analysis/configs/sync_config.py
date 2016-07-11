import logging
import emission.analysis.configs.config_utils as eacc

def get_config(user_id, time_query):
    # right now, we are not doing any server side overrides, so we pick the
    # last user defined configuration for this user
    SYNC_CONFIG_KEY = "config/sync_config"
    return eacc.get_last_entry(user_id, time_query, SYNC_CONFIG_KEY)
