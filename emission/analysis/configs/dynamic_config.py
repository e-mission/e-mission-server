import sys
import os
import logging

import json
import requests

STUDY_CONFIG = os.getenv('STUDY_CONFIG', "stage-program")

dynamic_config = None
def get_dynamic_config():
    global dynamic_config
    if dynamic_config is not None:
        logging.debug("Returning cached dynamic config for %s at version %s" % (STUDY_CONFIG, dynamic_config['version']))
        return dynamic_config
    logging.debug("No cached dynamic config for %s, downloading from server" % STUDY_CONFIG)
    download_url = "https://raw.githubusercontent.com/e-mission/nrel-openpath-deploy-configs/main/configs/" + STUDY_CONFIG + ".nrel-op.json"
    logging.debug("About to download config from %s" % download_url)
    r = requests.get(download_url)
    if r.status_code != 200:
        logging.debug(f"Unable to download study config, status code: {r.status_code}")
        # sys.exit(1)
        # TODO what to do here? What if Github is down or something?
        # If we terminate, will the pipeline just try again later?
    else:
        dynamic_config = json.loads(r.text)
        logging.debug(f"Successfully downloaded config with version {dynamic_config['version']} "\
            f"for {dynamic_config['intro']['translated_text']['en']['deployment_name']} "\
            f"and data collection URL {dynamic_config['server']['connectUrl']}")
        return dynamic_config
