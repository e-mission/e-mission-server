import logging
import requests
import copy

# Our imports
import emission.core.get_database as edb
import emission.net.ext_service.push.notify_interface as pni

def get_interface(push_config):
    return IonicPush(push_config)

class IonicPush(pni.NotifyInterface):
    def __init__(self, push_config):
        self.server_auth_token = push_config["server_auth_token"]

    def get_auth_header(self):
        logging.debug("Found server_auth_token starting with %s" % self.server_auth_token[0:10])
        return {
            'Authorization': "Bearer %s" % self.server_auth_token,
            'Content-Type': "application/json"
        }

    def send_msg_to_service(self, method, url, json_data):
        return requests.request(method, url, headers=self.get_auth_header(), json=json_data)

    def invalidate_entries(self, ret_tokens_list):
        for token_entry in ret_tokens_list:
            edb.get_profile_db().update({"device_token": token_entry["token"]}, {"$set": {
                "device_token_valid": token_entry["valid"],
                "device_token_invalidated": token_entry["invalidated"]
            }});

    def get_and_invalidate_entries(self):
        ret_tokens_list = self.send_msg_to_service("GET", "https://api.ionic.io/push/tokens", {})
        self.invalidate_entries(ret_tokens_list)

    def send_visible_notification(self, token_list, title, message, json_data, dev=False):
        if len(token_list) == 0:
            logging.info("len(token_list) == 0, early return to save api calls")
            return

        profile_to_use = "devpush" if dev == True else "prodpush";
        logging.debug("dev = %s, using profile = %s" % (dev, profile_to_use))

        message_dict = {
            "tokens": token_list,
            "profile": profile_to_use,
            "notification": {
                "title": title,
                "message": message,
                "android": {
                    "data": json_data,
                    "payload": json_data,
                },
                "ios": {
                    "data": json_data,
                    "payload": json_data
                }
            }
        }
        send_push_url = "https://api.ionic.io/push/notifications"
        response = self.send_msg_to_service("POST", send_push_url, message_dict)
        logging.debug(response)
        return response

    def send_silent_notification(token_list, json_data, dev=False):
        if len(token_list) == 0:
            logging.info("len(token_list) == 0, early return to save api calls")
            return

        ios_raw_data = copy.copy(json_data)
        # multiplying by 10^6 gives us the maximum resolution possible while still
        # being not a float. Have to see if that is too big.
        # Hopefully we will never send a push notification a millisecond to a single phone
        ios_raw_data.update({"notId": int(time.time() * 10**6)})
        ios_raw_data.update({"payload": ios_raw_data["notId"]})

        profile_to_use = "devpush" if dev == True else "prodpush";
        logging.debug("dev = %s, using profile = %s" % (dev, profile_to_use))

        message_dict = {
            "tokens": token_list,
            "profile": profile_to_use,
            "notification": {
                "android": {
                    "content_available": 1,
                    "data": json_data,
                    "payload": json_data
                },
                "ios": {
                    "content_available": 1,
                    "priority": 10,
                    "data": ios_raw_data,
                    "payload": ios_raw_data
                }
            }
        }
        send_push_url = "https://api.ionic.io/push/notifications"
        response = send_msg_to_service("POST", send_push_url, message_dict)
        logging.debug(response)
        return response

    def display_response(response):
        response_json = response.json()
        rjd = response_json["data"]
        logging.debug("ionic push result: created %s state %s status %s" % (rjd["created"],
                rjd["state"], rjd["status"]))
