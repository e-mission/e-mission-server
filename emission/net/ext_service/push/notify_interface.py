# Standard imports
import json
import copy
import requests
import logging
import uuid
import random
import time

# Our imports
import emission.core.get_database as edb
from pyfcm import FCMNotification

# Note that the URL is hardcoded because the API endpoints are not standardized.
# If we change a push provider, we will need to modify to match their endpoints.
# Hardcoding will remind us of this :)
# We can revisit this if push providers eventually decide to standardize...

try:
    key_file = open('conf/net/ext_service/push.json')
    key_data = json.load(key_file)
    server_auth_token = key_data["server_auth_token"]
except:
    logging.exception("push service not configured, push notifications not supported")

def get_auth_header():
    logging.debug("Found server_auth_token starting with %s" % server_auth_token[0:10])
    return {
        'Authorization': "Bearer %s" % server_auth_token,
        'Content-Type': "application/json"
    }

def send_msg_to_service(method, url, json_data):
    return requests.request(method, url, headers=get_auth_header(), json=json_data)

def invalidate_entries(ret_tokens_list):
    for token_entry in ret_tokens_list:
        edb.get_profile_db().update({"device_token": token_entry["token"]}, {"$set": {
            "device_token_valid": token_entry["valid"],
            "device_token_invalidated": token_entry["invalidated"]
        }});

def get_and_invalidate_entries():
    ret_tokens_list = send_msg_to_service("GET", "https://api.ionic.io/push/tokens", {})
    invalidate_entries(ret_tokens_list)

def send_visible_notification(token_list, title, message, json_data, dev=False):
    if len(token_list) == 0:
        logging.info("len(token_list) == 0, early return to save api calls")
        return

    profile_to_use = "devpush" if dev == True else "prodpush";
    logging.debug("dev = %s, using profile = %s" % (dev, profile_to_use))

    push_service = FCMNotification(api_key=server_auth_token)

#    message_dict = {
#        "tokens": token_list,
#        "profile": profile_to_use,
#        "notification": {
#            "title": title,
#            "message": message,
#            "android": {
#                "data": json_data,
#                "payload": json_data,
#            },
#            "ios": {
#                "data": json_data,
#                "payload": json_data
#            }
#        }
#    }
#    send_push_url = "https://api.ionic.io/push/notifications"
#    response = send_msg_to_service("POST", send_push_url, message_dict)
    data_message = {
       "data": json_data,
       "payload": json_data
    }
    response = push_service.notify_multiple_devices(registration_ids=token_list,
                                           message_body=message,
                                           message_title=title,
                                           data_message=data_message)
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

    push_service = FCMNotification(api_key=server_auth_token)

#    message_dict = {
#        "tokens": token_list,
#        "profile": profile_to_use,
#        "notification": {
#            "android": {
#                "content_available": 1,
#                "data": json_data,
#                "payload": json_data
#            },
#            "ios": {
#                "content_available": 1,
#                "priority": 10,
#                "data": ios_raw_data,
#                "payload": ios_raw_data
#            }
#        }
#    }
#    send_push_url = "https://api.ionic.io/push/notifications"
#    response = send_msg_to_service("POST", send_push_url, message_dict)
    response = push_service.notify_multiple_devices(registration_ids=token_list,
                                               data_message=ios_raw_data,
                                               content_available=True)
    logging.debug(response)
    return response
