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
import time
import pyfcm

# Our imports
import emission.core.get_database as edb
import emission.net.ext_service.push.notify_interface as pni
from pyfcm import FCMNotification


def get_interface(push_config):
    return FirebasePush(push_config)

class FirebasePush(pni.NotifyInterface):
    def __init__(self, push_config):
        self.service_account_file = push_config.get("PUSH_SERVICE_ACCOUNT_FILE")
        self.server_auth_token = push_config.get("PUSH_SERVER_AUTH_TOKEN")
        self.project_id = push_config.get("PUSH_PROJECT_ID")
        if "PUSH_APP_PACKAGE_NAME" in push_config:
            self.app_package_name = push_config.get("PUSH_APP_PACKAGE_NAME")
        else:
            logging.warning("No package name specified, defaulting to embase")
            self.app_package_name = "edu.berkeley.eecs.embase"
        self.is_fcm_format = push_config.get("PUSH_IOS_TOKEN_FORMAT") == "fcm"

    def get_and_invalidate_entries(self):
        # Need to figure out how to do this on firebase
        pass

    def notify_multiple_devices(self, push_service, fcm_token_list,
        notification_title=None, notification_body=None, data_payload=None):
        results = {}
        n_success = 0
        n_failure = 0
        for t in fcm_token_list:
            trunc_t = t[:10]
            try:
                result = push_service.notify(fcm_token=t,
                                             notification_title=notification_title,
                                             notification_body=notification_body,
                                             data_payload=data_payload)
                results.update({trunc_t: result['name']})
                n_success = n_success + 1
                print("Successfully sent to %s..." % (trunc_t))
            except (pyfcm.errors.FCMNotRegisteredError, pyfcm.errors.InvalidDataError) as e:
                results.update({trunc_t: str(e)})
                n_failure = n_failure + 1
                print("Found error %s while sending to token %s... skipping" % (e, trunc_t))
        response = {"success": n_success, "failure": n_failure, "results": results}
        print(response)
        logging.debug(response)
        return response

    @staticmethod
    def print_dev_flag_warning():
        logging.warning("dev flag is ignored for firebase, since the API does not distinguish between production and dev")
        logging.warning("https://stackoverflow.com/questions/38581241/ios-firebase-push-notification-for-sandbox-only")
        logging.warning("https://firebase.google.com/docs/reference/fcm/rest/v1/projects.messages")

    def map_existing_fcm_tokens(self, token_map):
        if self.is_fcm_format:
            logging.info("iOS tokens are already in the FCM format, no mapping required")
            return ({"ios": token_map["ios"],
                    "android": token_map["android"]}, [])
        # android tokens never need to be mapped, so let's just not even check them
        mapped_token_map = {"ios": [],
                            "android": token_map["android"]}
        unmapped_token_list = []

        for token in token_map["ios"]:
            existing_mapping = edb.get_push_token_mapping_db().find_one({"native_token": token})
            if existing_mapping is not None:
                assert(existing_mapping["native_token"] == token)
                mapped_token = existing_mapping["mapped_token"]
                mapped_platform = existing_mapping["platform"]
                # we are only iterating over ios mappings anyway
                logging.debug("%s: mapped %s -> %s" % (mapped_platform, token, mapped_token))
                assert(mapped_platform == "ios")
                mapped_token_map[mapped_platform].append(mapped_token)
            else:
                logging.debug("No mapping found for token %s, need to query from database" % token)
                unmapped_token_list.append(token)
        return (mapped_token_map, unmapped_token_list)

    def retrieve_fcm_tokens(self, token_list, dev):
        if len(token_list) == 0:
            logging.debug("len(token_list) == 0, skipping fcm token mapping to save API call")
            return []
        importedResultList = []
        importHeaders = {"Authorization": "key=%s" % self.server_auth_token,
                         "Content-Type": "application/json"}
        for curr_first in range(0, len(token_list), 100):
            curr_batch = token_list[curr_first:curr_first + 100]
            importMessage = {
                "application": self.app_package_name,
                "sandbox": dev,
                "apns_tokens":curr_batch
            }
            logging.debug("About to send message %s" % importMessage)
            importResponse = requests.post("https://iid.googleapis.com/iid/v1:batchImport", headers=importHeaders, json=importMessage)
            logging.debug("Response = %s" % importResponse)
            importedResultJSON = importResponse.json()
            if importedResultJSON is not None and "results" in importedResultJSON:
                importedResult = importedResultJSON["results"]
                importedResultList.extend(importedResult)
                print("After appending result of size %s, total size = %s" %
                        (len(importedResult), len(importedResultList)))
            else:
                print(f"Received invalid result for batch starting at = {curr_first}")
        return importedResultList

    def process_fcm_token_result(self, importedResultList):
        ret_list = []
        for i, result in enumerate(importedResultList):
            if result["status"] == "OK" and "registration_token" in result:
                ret_list.append(result["registration_token"])
                logging.debug("Found firebase mapping from %s -> %s at index %d"%
                    (result["apns_token"], result["registration_token"], i));
                edb.get_push_token_mapping_db().insert_one({"native_token": result["apns_token"],
                                                        "platform": "ios",
                                                        "mapped_token": result["registration_token"]})
            else:
                logging.warning("Got error %s while mapping iOS token at index %d" %
                    (result, i));
        return ret_list

    def convert_to_fcm_if_necessary(self, token_map, dev):
        (mapped_token_map, unmapped_token_list) = self.map_existing_fcm_tokens(token_map)
        importedResultList = self.retrieve_fcm_tokens(unmapped_token_list, dev)
        newly_mapped_token_list = self.process_fcm_token_result(importedResultList)
        print("after mapping iOS tokens, imported %s -> processed %s" %
            (len(importedResultList), len(newly_mapped_token_list)))
        combo_token_map = {"ios": [],
                           "android": []}
        combo_token_map["ios"] = mapped_token_map["ios"] + newly_mapped_token_list
        combo_token_map["android"] = mapped_token_map["android"]
        print("combo token map has %d ios entries and %d android entries" %
            (len(combo_token_map["ios"]), len(combo_token_map["android"])))
        return combo_token_map

    def send_visible_notification(self, token_map, title, message, json_data, dev=False):
        if len(token_map) == 0:
            logging.info("len(token_map) == 0, early return to save api calls")
            return

        # convert tokens if necessary
        fcm_token_map = self.convert_to_fcm_if_necessary(token_map, dev)

        push_service = FCMNotification(
            service_account_file=self.service_account_file,
            project_id=self.project_id)
        # Send android and iOS messages separately because they have slightly
        # different formats
        # https://github.com/e-mission/e-mission-server/issues/564#issuecomment-360720598
        android_response = self.notify_multiple_devices(push_service,
                                               fcm_token_map["android"],
                                               notification_body = message,
                                               notification_title = title,
                                               data_payload = json_data)
        ios_response = self.notify_multiple_devices(push_service,
                                               fcm_token_map["ios"],
                                               notification_body = message,
                                               notification_title = title,
                                               data_payload = json_data)
        combo_response = {"ios": ios_response, "android": android_response}
        logging.debug(combo_response)
        return combo_response

    def send_silent_notification(self, token_map, json_data, dev=False):
        if len(token_map) == 0:
            logging.info("len(token_map) == 0, early return to save api calls")
            return

        ios_raw_data = copy.copy(json_data)
        # multiplying by 10^6 gives us the maximum resolution possible while still
        # being not a float. Have to see if that is too big.
        # Hopefully we will never send a push notification a millisecond to a single phone
        ios_raw_data.update({"notId": str(int(time.time() * 10**6))})
        ios_raw_data.update({"payload": ios_raw_data["notId"]})

        push_service = FCMNotification(
            service_account_file=self.service_account_file,
            project_id=self.project_id)

        # convert tokens if necessary
        fcm_token_map = self.convert_to_fcm_if_necessary(token_map, dev)

        response = {}
        response["ios"] = self.notify_multiple_devices(push_service,
                            fcm_token_map["ios"], data_payload=ios_raw_data)

        response["android"] = {"success": "skipped", "failure": "skipped",
                               "results": "skipped"}
        print(response)
        logging.debug(response)
        return response

    def display_response(self, response):
        response_ios = response["ios"]
        response_android = response['android']

        logging.debug("firebase push result for ios: success %s failure %s results %s" %
            (response_ios["success"], response_ios["failure"], response_ios["results"]))
        logging.debug("firebase push result for android: success %s failure %s results %s" %
            (response_android["success"], response_android["failure"], response_android["results"]))
