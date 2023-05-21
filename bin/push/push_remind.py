import json
import logging
import os
import requests

import emission.storage.decorations.user_queries as esdu
import emission.net.ext_service.push.notify_usage as pnu

STUDY_CONFIG = os.getenv('STUDY_CONFIG', "stage-program")

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    logging.debug(f"STUDY_CONFIG is {STUDY_CONFIG}")

    STUDY_CONFIG = os.getenv('STUDY_CONFIG', "stage-study")

    download_url = "https://raw.githubusercontent.com/e-mission/nrel-openpath-deploy-configs/main/configs/" + STUDY_CONFIG + ".nrel-op.json"
    logging.debug("About to download config from %s" % download_url)
    r = requests.get(download_url)
    if r.status_code is not 200:
        logging.debug(f"Unable to download study config, status code: {r.status_code}")
        sys.exit(1)
    else:
        dynamic_config = json.loads(r.text)
        logging.debug(f"Successfully downloaded config with version {dynamic_config['version']} "\
            f"for {dynamic_config['intro']['translated_text']['en']['deployment_name']} "\
            f"and data collection URL {dynamic_config['server']['connectUrl']}")
        
        if "reminderSchemes" in dynamic_config:
            logging.debug("Found flexible notification configuration, skipping server-side push")
        else:
            uuid_list = esdu.get_all_uuids()
            json_data = {
                "title": "Trip labels requested",
                "message": "Please label your trips for the day"
            }
            response = pnu.send_visible_notification_to_users(uuid_list,
                                                                json_data["title"],
                                                                json_data["message"],
                                                                json_data,
                                                                dev = False)

