import json
import logging
import os

def get_config_data_from_env():
    config_data_env = {
        "static_path": os.getenv('WEB_SERVER_STATIC_PATH', "webapp/www/"),
        "server_host": os.getenv('WEB_SERVER_HOST', "0.0.0.0"),
        "server_port": os.getenv('WEB_SERVER_PORT', "8080"),
        "socket_timeout": os.getenv('WEB_SERVER_TIMEOUT', "3600"),
        "auth_method": os.getenv('WEB_SERVER_AUTH', "skip"),
        "aggregate_call_auth": os.getenv('WEB_SERVER_AGGREGATE_CALL_AUTH', "no_auth"),
        "not_found_redirect": os.getenv('WEB_SERVER_REDIRECT_URL', "https://www.nrel.gov/transportation/openpath.html")
    }
    return config_data_env

def check_unset_env_vars():
    config_data_env = {
        "static_path": os.getenv('WEB_SERVER_STATIC_PATH'),
        "server_host": os.getenv('WEB_SERVER_HOST'),
        "server_port": os.getenv('WEB_SERVER_PORT'),
        "socket_timeout": os.getenv('WEB_SERVER_TIMEOUT'),
        "auth_method": os.getenv('WEB_SERVER_AUTH'),
        "aggregate_call_auth": os.getenv('WEB_SERVER_AGGREGATE_CALL_AUTH'),
        "not_found_redirect": os.getenv('WEB_SERVER_REDIRECT_URL')
    }
    return not any(config_data_env.values())

def get_config_data():
    try:
        config_file = open('conf/storage/db.conf')
        ret_val = json.load(config_file)
        config_file.close()
    except:
        # Check if all Webserver environment variables are not set
        # if check_unset_env_vars():
        logging.debug("webserver not configured, falling back to sample, default configuration")
        ret_val = get_config_data_from_env()
    return ret_val

config_data = get_config_data()

def get_config():
    return config_data

def reload_config():
    global config_data
    config_data = get_config_data()
