# Standard imports
import json
import logging
import uuid
import requests

# Our imports
import emission.core.get_database as edb

try:
    key_file = open('conf/net/ext_service/push.json')
    key_data = json.load(key_file)
    server_auth_token = key_data["server_auth_token"]
except:
    logging.exception("push service not configured, push notifications not supported")

def get_platform_query(platform):
    return {"curr_platform": platform}

def get_sync_interval_query(interval):
    return {"curr_sync_interval": interval}

def get_user_query(user_id_list):
    return {"user_id": {"$in": user_id_list}}

def combine_queries(query_list):
    combined_query = {}
    for query in query_list:
        combined_query.update(query)
    return combined_query

def convert_to_fcm_if_necessary(token_list):
    importHeaders = {"Authorization": "key=%s" % server_auth_token,
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
    ret_list = []
    if "results" in importedResultJSON:
        importedResult = importedResultJSON["results"]
        for i, result in enumerate(importedResult):
            if result["status"] == "OK" and "registration_token" in result:
                ret_list.append(result["registration_token"])
                logging.debug("Found firebase mapping from %s -> %s at index %d"%
    		(result["apns_token"], result["registration_token"], i));
            else:
                logging.debug("Must already be android token, leave it unchanged");
                ret_list.append(token_list[i])
    return ret_list

def get_matching_tokens(query):
    logging.debug("Getting tokens matching query %s" % query)
    ret_cursor = edb.get_profile_db().find(query, {"_id": False, "device_token": True})
    mapped_list = map(lambda e: e.get("device_token"), ret_cursor)
    non_null_list = [item for item in mapped_list if item is not None]
    fcm_mapped_list = convert_to_fcm_if_necessary(non_null_list)
    return fcm_mapped_list

def get_matching_user_ids(query):
    logging.debug("Getting tokens matching query %s" % query)
    ret_cursor = edb.get_profile_db().find(query, {"_id": False, "user_id": True})
    mapped_list = map(lambda e: e.get("user_id"), ret_cursor)
    ret_list = [item for item in mapped_list if item is not None]
    return ret_list
