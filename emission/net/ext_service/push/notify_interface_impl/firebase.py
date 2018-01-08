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
        mapped_token_list = []
        unmapped_token_list = []

        for token in token_list:
            existing_mapping = edb.get_push_token_mapping_db().find_one({"native_token": token})
            if existing_mapping is not None:
                assert(existing_mapping["native_token"] == token)
                mapped_token = existing_mapping["mapped_token"]
                logging.debug("mapped %s -> %s" % (token, mapped_token))
                mapped_token_list.append(mapped_token)
            else:
                logging.debug("No mapping found for token %s, need to query from database" % token)
                unmapped_token_list.append(token)
        return (mapped_token_list, unmapped_token_list)

    def retrieve_fcm_tokens(self, token_list):
        importHeaders = {"Authorization": "key=%s" % self.server_auth_token,
                         "Content-Type": "application/json"}
        importMessage = {
            "application": "edu.berkeley.eecs.emission",
            "sandbox": False,
            "apns_tokens":token_list
        }
        logging.debug("About to send message %s" % importMessage)
        importResponse = requests.post("https://iid.googleapis.com/iid/v1:batchImport", headers=importHeaders, json=importMessage)
        logging.debug("Response = %s" % importResponse)
        importedResultJSON = importResponse.json()
        return importedResultJSON

    def process_fcm_token_result(self, token_list, importedResultJSON):
        ret_list = []
        if "results" in importedResultJSON:
            importedResult = importedResultJSON["results"]
            for i, result in enumerate(importedResult):
                if result["status"] == "OK" and "registration_token" in result:
                    ret_list.append(result["registration_token"])
                    logging.debug("Found firebase mapping from %s -> %s at index %d"%
                        (result["apns_token"], result["registration_token"], i));
                    edb.get_push_token_mapping_db().insert({"native_token": result["apns_token"],
                                                            "mapped_token": result["registration_token"]})
                else:
                    logging.debug("Must already be android token, leave it unchanged");
                    # TODO: Determine whether to store a mapping here or not depending on what the result
                    # for an android token is
                    ret_list.append(token_list[i])
        return ret_list

    def convert_to_fcm_if_necessary(self, token_list):
        (mapped_token_list, unmapped_token_list) = self.map_existing_fcm_tokens(token_list)
        importedResultJSON = self.retrieve_fcm_tokens(unmapped_token_list)
        newly_mapped_token_list = self.process_fcm_token_list(token_list, importedResultJSON)
        return mapped_token_list + newly_mapped_token_list

    def send_visible_notification(self, token_list, title, message, json_data, dev=False):
        if len(token_list) == 0:
            logging.info("len(token_list) == 0, early return to save api calls")
            return

        FirebasePush.print_dev_flag_warning()

        # convert tokens if necessary
        fcm_token_list = self.convert_to_fcm_if_necessary(token_list)

        push_service = FCMNotification(api_key=server_auth_token)
        data_message = {
           "data": json_data,
           "payload": json_data
        }
        response = push_service.notify_multiple_devices(registration_ids=fcm_token_list,
                                               message_body=message,
                                               message_title=title,
                                               data_message=data_message)
        logging.debug(response)
        return response

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

        push_service = FCMNotification(api_key=server_auth_token)

        FirebasePush.print_dev_flag_warning()
        # convert tokens if necessary
        fcm_token_list = self.convert_to_fcm_if_necessary(token_list)

        response = push_service.notify_multiple_devices(registration_ids=fcm_token_list,
                                                   data_message=ios_raw_data,
                                                   content_available=True)
        logging.debug(response)
        return response

    def display_response(response):
        response_json = response
        logging.debug("firebase push result: success %s failure %s results %s" %
            (response_json["success"], response_json["failure"], response_json["results"]))
