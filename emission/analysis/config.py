import json

def get_config_data():
    try:
        config_file = open('conf/analysis/debug.conf.json')
    except:
        print("analysis.debug.conf.json not configured, falling back to sample, default configuration")
        config_file = open('conf/analysis/debug.conf.json.sample')
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
