import json
import logging
import os

def get_config_data_from_env():
    config_data_env = {
        "timeseries": {
            "url": os.getenv('DB_HOST', "localhost"),
            "result_limit": os.getenv('DB_TS_RESULT_LIMIT', 250000)
        }
    }
    return config_data_env

def check_unset_env_vars():
    config_data_env = {
        "timeseries": {
            "url": os.getenv('DB_HOST'),
            "result_limit": os.getenv('DB_TS_RESULT_LIMIT')
        }
    }
    return not any(config_data_env.values())

def get_config_data():
    try:
        config_file = open('conf/storage/db.conf')
        ret_val = json.load(config_file)
        config_file.close()
    except:
        # Check if all DB environment variables are not set
        # if check_unset_env_vars():
            # print("All DB environment variables are set to None")
        ret_val = get_config_data_from_env()
        if ret_val["timeseries"]["url"] == "localhost":
            print("storage not configured, falling back to sample, default configuration")
    return ret_val

config_data = get_config_data()

def get_config():
    return config_data

def reload_config():
    global config_data
    config_data = get_config_data()
