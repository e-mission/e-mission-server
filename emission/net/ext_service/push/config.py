import json
import logging
import os

def get_config_data_from_env():
    config_data_env = {
        "provider": os.getenv("PUSH_PROVIDER"),
        "server_auth_token": os.getenv("PUSH_SERVER_AUTH_TOKEN"),
        "app_package_name": os.getenv("PUSH_APP_PACKAGE_NAME"),
        "ios_token_format": os.getenv("PUSH_IOS_TOKEN_FORMAT")
    }
    return config_data_env

def get_config_data():
    try:
        config_file = open('conf/net/ext_service/push.json')
        ret_val = json.load(config_file)
        config_file.close()
    except:
        logging.debug("net.ext_service.push.json not configured, checking environment variables...")
        ret_val = get_config_data_from_env()
        # Check if all PUSH environment variables are not set
        if (not any(ret_val.values())):
            raise TypeError
    return ret_val

try:
    config_data = get_config_data()
except:
    logging.debug("All push environment variables are set to None")

def get_config():
    return config_data

def reload_config():
    global config_data
    config_data = get_config_data()
