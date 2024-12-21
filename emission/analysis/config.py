import json
import os

ANALYSIS_CONF_PATH = "conf/analysis/debug.conf.json"
ANALYSIS_CONF_PROD_PATH = "conf/analysis/debug.conf.prod.json"
ANALYSIS_CONF_DEV_PATH = "conf/analysis/debug.conf.dev.json"

def get_config_data():
    try:
        print("Trying to open debug.conf.json")
        config_file = open(ANALYSIS_CONF_PATH)
    except:
        if os.getenv("PROD_STAGE") == "TRUE":
            print("In production environment, config not overridden, using default production debug.conf")
            config_file = open(ANALYSIS_CONF_PROD_PATH)
        else:
            print("analysis.debug.conf.json not configured, falling back to sample, default configuration")
            config_file = open(ANALYSIS_CONF_DEV_PATH)
    ret_val = json.load(config_file)
    config_file.close()
    return ret_val

config_data = get_config_data()

def get_config():
    return config_data

def reload_config():
    global config_data
    config_data = get_config_data()

def get_section_key_for_analysis_results():
    return config_data["analysis.result.section.key"]
