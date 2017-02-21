# Standard imports
import json
import requests
import logging
import uuid
import random

# Our imports
import emission.core.get_database as edb

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
    message_dict = {
        "tokens": token_list,
        "profile": "devpush",
        "notification": {
            "title": title,
            "message": message,  # but on android, the title and message are null!
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
    response = send_msg_to_service("POST", send_push_url, message_dict)
    logging.debug(response)
    return response
    
def send_silent_notification(token_list, json_data, dev=False):
    message_dict = {
        "tokens": token_list,
        "profile": "devpush",
        "notification": {
            "android": {
                "content_available": 1,
                "data": json_data,
                "payload": json_data
            },
            "ios": {
                "content_available": 1,
                "priority": 10,
                "data": json_data,
                "payload": json_data
            }
        }
    }
    send_push_url = "https://api.ionic.io/push/notifications"
    response = send_msg_to_service("POST", send_push_url, message_dict)
    logging.debug(response)
    return response
