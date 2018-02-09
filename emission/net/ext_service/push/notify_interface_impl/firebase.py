from __future__ import unicode_literals
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import
from future import standard_library
standard_library.install_aliases()
from builtins import *
import logging
import requests
import copy

# Our imports
import emission.core.get_database as edb
import emission.net.ext_service.push.notify_interface as pni
from pyfcm import FCMNotification


def get_interface(push_config):
    return FirebasePush(push_config)

class FirebasePush(pni.NotifyInterface):
    def __init__(self, push_config):
        self.server_auth_token = push_config["server_auth_token"]

    def get_and_invalidate_entries(self):
        # Need to figure out how to do this on firebase
        pass

    @staticmethod
    def print_dev_flag_warning():
        logging.warning("dev flag is ignored for firebase, since the API does not distinguish between production and dev")
        logging.warning("https://stackoverflow.com/questions/38581241/ios-firebase-push-notification-for-sandbox-only")
        logging.warning("https://firebase.google.com/docs/reference/fcm/rest/v1/projects.messages")

    def map_existing_fcm_tokens(self, token_list):
        mapped_token_map = {"ios": [],
                            "android": []}
        unmapped_token_list = []

        for token in token_list:
            existing_mapping = edb.get_push_token_mapping_db().find_one({"native_token": token})
            if existing_mapping is not None:
                assert(existing_mapping["native_token"] == token)
                mapped_token = existing_mapping["mapped_token"]
                mapped_platform = existing_mapping["platform"]
                logging.debug("%s: mapped %s -> %s" % (mapped_platform, token, mapped_token))
                mapped_token_map[mapped_platform].append(mapped_token)
            else:
                logging.debug("No mapping found for token %s, need to query from database" % token)
                unmapped_token_list.append(token)
        return (mapped_token_map, unmapped_token_list)

    def retrieve_fcm_tokens(self, token_list, dev):
        importHeaders = {"Authorization": "key=%s" % self.server_auth_token,
                         "Content-Type": "application/json"}
        importMessage = {
            "application": "edu.berkeley.eecs.emission",
            "sandbox": dev,
            "apns_tokens":token_list
        }
        logging.debug("About to send message %s" % importMessage)
        importResponse = requests.post("https://iid.googleapis.com/iid/v1:batchImport", headers=importHeaders, json=importMessage)
        logging.debug("Response = %s" % importResponse)
        importedResultJSON = importResponse.json()
        return importedResultJSON

    def process_fcm_token_result(self, token_list, importedResultJSON):
        ret_map = {"ios": [],
                   "android": []}
        if "results" in importedResultJSON:
            importedResult = importedResultJSON["results"]
            for i, result in enumerate(importedResult):
                if result["status"] == "OK" and "registration_token" in result:
                    ret_map["ios"].append(result["registration_token"])
                    logging.debug("Found firebase mapping from %s -> %s at index %d"%
                        (result["apns_token"], result["registration_token"], i));
                    edb.get_push_token_mapping_db().insert({"native_token": result["apns_token"],
                                                            "platform": "ios",
                                                            "mapped_token": result["registration_token"]})
                else:
                    logging.debug("Must already be android token, leave it unchanged");
                    # TODO: Determine whether to store a mapping here or not depending on what the result
                    # for an android token is
                    ret_map["android"].append(token_list[i])
        return ret_map

    def convert_to_fcm_if_necessary(self, token_list, dev):
        (mapped_token_map, unmapped_token_list) = self.map_existing_fcm_tokens(token_list)
        importedResultJSON = self.retrieve_fcm_tokens(unmapped_token_list, dev)
        newly_mapped_token_map = self.process_fcm_token_result(token_list, importedResultJSON)
        combo_token_map = {"ios": [],
                           "android": []}
        combo_token_map["ios"] = mapped_token_map["ios"] + newly_mapped_token_map["ios"]
        combo_token_map["android"] = mapped_token_map["android"] + newly_mapped_token_map["android"]
        return combo_token_map

    def send_visible_notification(self, token_list, title, message, json_data, dev=False):
        if len(token_list) == 0:
            logging.info("len(token_list) == 0, early return to save api calls")
            return

        # convert tokens if necessary
        fcm_token_map = self.convert_to_fcm_if_necessary(token_list, dev)

        push_service = FCMNotification(api_key=self.server_auth_token)
        data_message = {
           "data": json_data,
           "payload": json_data
        }
        # Send android and iOS messages separately because they have slightly
        # different formats
        # https://github.com/e-mission/e-mission-server/issues/564#issuecomment-360720598
        android_response = push_service.notify_multiple_devices(registration_ids=fcm_token_map["android"],
                                               data_message=data_message)
        ios_response = push_service.notify_multiple_devices(registration_ids=fcm_token_map["ios"],
                                               message_body = message,
                                               message_title = title,
                                               data_message=data_message)
        combo_response = {"ios": ios_response, "android": android_response}
        logging.debug(combo_response)
        return combo_response

    def send_silent_notification(self, token_list, json_data, dev=False):
        if len(token_list) == 0:
            logging.info("len(token_list) == 0, early return to save api calls")
            return

        ios_raw_data = copy.copy(json_data)
        # multiplying by 10^6 gives us the maximum resolution possible while still
        # being not a float. Have to see if that is too big.
        # Hopefully we will never send a push notification a millisecond to a single phone
        ios_raw_data.update({"notId": int(time.time() * 10**6)})
        ios_raw_data.update({"payload": ios_raw_data["notId"]})

        push_service = FCMNotification(api_key=self.server_auth_token)

        # convert tokens if necessary
        fcm_token_list = self.convert_to_fcm_if_necessary(token_list, dev)

        response = push_service.notify_multiple_devices(registration_ids=fcm_token_list,
                                                   data_message=ios_raw_data,
                                                   content_available=True)
        logging.debug(response)
        return response

    def display_response(self, response):
        response_ios = response["ios"]
        response_android = response['android']

        logging.debug("firebase push result for ios: success %s failure %s results %s" %
            (response_ios["success"], response_ios["failure"], response_ios["results"]))
        logging.debug("firebase push result for android: success %s failure %s results %s" %
            (response_android["success"], response_android["failure"], response_android["results"]))
