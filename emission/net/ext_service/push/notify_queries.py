# Standard imports
import json
import logging
import uuid

# Our imports
import emission.core.get_database as edb

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

def get_matching_tokens(query):
    logging.debug("Getting tokens matching query %s" % query)
    ret_cursor = edb.get_profile_db().find(query, {"_id": False, "device_token": True})
    mapped_list = map(lambda e: e.get("device_token"), ret_cursor)
    ret_list = [item for item in mapped_list if item is not None]
    return ret_list

def get_matching_user_ids(query):
    logging.debug("Getting tokens matching query %s" % query)
    ret_cursor = edb.get_profile_db().find(query, {"_id": False, "user_id": True})
    mapped_list = map(lambda e: e.get("user_id"), ret_cursor)
    ret_list = [item for item in mapped_list if item is not None]
    return ret_list
