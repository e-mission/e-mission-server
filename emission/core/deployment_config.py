import os
import logging
import json
import requests

STUDY_CONFIG = os.getenv('STUDY_CONFIG', "stage-program")
CONFIGS_URL = os.getenv('CONFIGS_URL', "https://raw.githubusercontent.com/e-mission/op-deployment-configs/main/configs/")

OP_CONFIG_EXTENSION = ".nrel-op.json"

deployment_config = None

def get_deployment_config(study_config=STUDY_CONFIG, configs_url=CONFIGS_URL, default=None):
    """
    Fetch the deployment config for the specified study config. Caches the result in memory to avoid repeated downloads.

    :param study_config: Name of the study config to fetch. Defaults to the value of the STUDY_CONFIG environment variable.
    :param configs_url: URL location of the config files. Defaults to GitHub; can be overridden for testing with locally served configs.
    :param default: Default value to return if the config cannot be fetched.
    :return: Deployment config dictionary or default value.
    """
    global deployment_config
    if deployment_config is not None:
        logging.debug("Returning cached deployment config for %s at version %s" % (study_config, deployment_config['version']))
        return deployment_config
    logging.debug("No cached deployment config for %s, downloading from server" % study_config)
    download_url = configs_url + study_config + OP_CONFIG_EXTENSION
    logging.debug("About to download config from %s" % download_url)
    r = requests.get(download_url)
    if r.status_code != 200:
        logging.debug(f"Unable to download study config, status code: {r.status_code}")
        return default
    else:
        deployment_config = json.loads(r.text)
        logging.debug(f"Successfully downloaded config with version {deployment_config['version']} "\
            f"for {deployment_config['intro']['translated_text']['en']['deployment_name']} "\
            f"and data collection URL {deployment_config['server']['connectUrl']}")
        return deployment_config
