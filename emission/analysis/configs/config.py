import logging
import importlib

import emission.net.usercache.abstract_usercache as enua

config_list = ["sensor_config"]

def save_all_configs(user_id, time_query):
    uc = enua.UserCache.getUserCache(user_id)
    config_name = config_list[0]
    return save_config(user_id, uc, time_query, config_name)

def save_config(user_id, uc, time_query, module_name):
    config_fn = get_configurator("emission.analysis.configs.%s" % module_name)
    logging.debug("for %s, config_fn = %s" % (module_name, config_fn))
    (config, last_processed_ts) = config_fn(user_id, time_query)
    logging.debug("for %s, config is %s" % (user_id, config))
    if config is not None:
        uc.putDocument("config/%s" % module_name, config)
        return last_processed_ts
    else:
        return None

def get_configurator(module_name):
    module = importlib.import_module(module_name)
    return getattr(module, "get_config")
