import logging
import importlib

config_list = ["sensor_config"]

def save_all_configs(user_id):
    uc = enua.UserCache.getUserCache(self.user_id)
    for config_name in config_list:
        save_config(user_id, uc, config_name)

def save_config(user_id, uc, module_name):
    config_fn = get_configurator(module_name)
    logging.debug("for %s, config_fn = %s" % (module_name, config_fn))
    config = config_fn(user_id)
    logging.debug("for %s, config is %s" % (user_id, config))
    if config is not None:
        uc.putDocument("config/%s" % module_name, config)

def get_configurator(module_name):
    module = importlib.import_module(module_name)
    return getattr(module, "get_config")
