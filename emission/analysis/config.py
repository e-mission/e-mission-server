import json

try:
    config_file = open('conf/analysis/debug.conf.json')
except:
    print("debug not configured, falling back to sample, default configuration")
    config_file = open('conf/analysis/debug.conf.json.sample')

config_data = json.load(config_file)

def get_config():
    return config_data
