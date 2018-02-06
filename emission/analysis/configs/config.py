from __future__ import unicode_literals
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import
from future import standard_library
standard_library.install_aliases()
from builtins import *
import logging
import importlib

import emission.net.usercache.abstract_usercache as enua

config_list = ["sensor_config", "sync_config", "consent"]

def save_all_configs(user_id, time_query):
    uc = enua.UserCache.getUserCache(user_id)
    # This is tricky to extend to beyond one type of config because it is not
    # clear which ts we should return. We cannot return two values because only
    # one of them can be stored in our current pipeline states.  We should be
    # able to return the max of them, since these are all from the same user,
    # so if we have received a later config, we know that we have processed all 
    # prior configs
    # Let's test it out and see if it works!
    last_processed_ts_list = [save_config(user_id, uc, time_query, config_name) 
        for config_name in config_list]
    logging.debug("last_processed_ts_list = %s" % last_processed_ts_list)
    return max(last_processed_ts_list)

def save_config(user_id, uc, time_query, module_name):
    config_fn = get_configurator("emission.analysis.configs.%s" % module_name)
    logging.debug("for %s, config_fn = %s" % (module_name, config_fn))
    (config, last_processed_ts) = config_fn(user_id, time_query)
    logging.debug("for %s, config is %s" % (user_id, config))
    if config is not None:
        uc.putDocument("config/%s" % module_name, config)
        return last_processed_ts
    else:
        return 0

def get_configurator(module_name):
    module = importlib.import_module(module_name)
    return getattr(module, "get_config")
