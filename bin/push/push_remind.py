import arrow
import json
import logging
logging.basicConfig(level=logging.INFO)
import os
import requests
import sys

import emission.core.get_database as edb
import emission.storage.decorations.analysis_timeseries_queries as esda
import emission.storage.decorations.user_queries as esdu
import emission.storage.timeseries.timequery as estt
import emission.net.ext_service.push.notify_usage as pnu

STUDY_CONFIG = os.getenv('STUDY_CONFIG', "stage-program")


def users_without_recent_user_input(uuid_list, recent_user_input_threshold=None):
    if recent_user_input_threshold is None:
        logging.debug("No recent_user_input_threshold provided, returning all users")
        return uuid_list
    now = arrow.now()
    tq = estt.TimeQuery(
        "data.start_ts",
        now.shift(days=-recent_user_input_threshold).int_timestamp,
        now.int_timestamp
    )
    filtered_uuids = []
    for user_id in uuid_list:
        trips = esda.get_entries(esda.CONFIRMED_TRIP_KEY, user_id, tq)
        for trip in trips:
            # If the trip's user_input is blank, it will be an empty dict {} which is falsy.
            # A slight caveat to this is that if the trip is partially labeled (i.e. they
            # labeled 'Mode' but not 'Purpose'), it will be non-empty and will be considered
            # the same as if it was fully labeled.
            # I think this is fine because if a user has partially labeled a trip, they have
            # already seen it and bugging them again is not likely to help.
            if not trip['data']['user_input']: # empty user_input is {} which is falsy
                logging.debug(f"User {user_id} has trip with no user input: {trip['_id']}")
                filtered_uuids.append(user_id)
                break
    return filtered_uuids


def bin_users_by_lang(uuid_list, langs, lang_key='phone_lang'):
    uuids_by_lang = {lang: [] for lang in langs}
    for user_id in uuid_list:
        user_profile = edb.get_profile_db().find_one({'user_id': user_id})
        user_lang = user_profile.get(lang_key) if user_profile else None
        logging.debug(f"User {user_id} has phone language {user_lang}")
        if user_lang not in uuids_by_lang:
            logging.debug(f"{user_lang} was not one of the provided langs, defaulting to en")
            user_lang = "en"
        uuids_by_lang[user_lang].append(user_id)
    return uuids_by_lang


if __name__ == '__main__':
    logging.debug(f"STUDY_CONFIG is {STUDY_CONFIG}")

    STUDY_CONFIG = os.getenv('STUDY_CONFIG', "stage-study")

    download_url = "https://raw.githubusercontent.com/e-mission/nrel-openpath-deploy-configs/main/configs/" + STUDY_CONFIG + ".nrel-op.json"
    logging.debug("About to download config from %s" % download_url)
    r = requests.get(download_url)
    if r.status_code != 200:
        logging.debug(f"Unable to download study config, status code: {r.status_code}")
        sys.exit(1)
    
    dynamic_config = json.loads(r.text)
    logging.info(f"Successfully downloaded config with version {dynamic_config['version']} "\
        f"for {dynamic_config['intro']['translated_text']['en']['deployment_name']} "\
        f"and data collection URL {dynamic_config['server']['connectUrl']}")
    
    if "reminderSchemes" in dynamic_config:
        logging.info("Found flexible notification configuration, skipping server-side push")
        sys.exit(0)

    # get push notification config (if not present in dynamic_config, use default)
    push_config = dynamic_config.get('push_notifications', {
        "title": {
            "en": "Trip labels requested",
            "es": "Etiquetas de viaje solicitadas",
        },
        "message": {
            "en": "Please label your recent trips",
            "es": "Por favor etiquete sus viajes recientes",
        },
        "recent_user_input_threshold": 7, # past week
    })

    # filter users based on recent user input and bin by language
    filtered_uuids = users_without_recent_user_input(
        esdu.get_all_uuids(),
        push_config.get('recent_user_input_threshold')
    )
    filtered_uuids_by_lang = bin_users_by_lang(filtered_uuids, push_config['title'].keys())

    # for each language, send a push notification to the selected users in that language
    for lang, uuids_to_notify in filtered_uuids_by_lang.items():
        if len(uuids_to_notify) == 0:
            logging.info(f"No users to notify in lang {lang}")
            continue
        logging.info(f"Sending push notifications to {len(uuids_to_notify)} users in lang {lang}")
        json_data = {
            "title": push_config["title"][lang],
            "message": push_config["message"][lang],    
        }
        response = pnu.send_visible_notification_to_users(uuids_to_notify,
                                                          json_data["title"],
                                                          json_data["message"],
                                                          json_data,
                                                          dev = False)
