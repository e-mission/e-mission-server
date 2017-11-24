# Standard imports
import json
import requests
import logging
import uuid
import random

# Our imports
import emission.net.ext_service.push.notify_interface as pni
import emission.net.ext_service.push.notify_queries as pnq

def __get_default_interface():
    interface_obj = pni.NotifyInterfaceFactory.getNotifyInterface(pushProvider)
    logging.debug("interface_obj = %s" % interface_obj)
    return interface_obj

def send_visible_notification_to_users(user_id_list, title, message, json_data, dev=False):
    token_list = pnq.get_matching_tokens(pnq.get_user_query(user_id_list))
    logging.debug("user_id_list of length %d -> token list of length %d" % 
        (len(user_id_list), len(token_list)))
    return __get_default_interface().send_visible_notification(token_list, title, message, json_data, dev)

def send_silent_notification_to_users(user_id_list, json_data, dev=False):
    token_list = pnq.get_matching_tokens(pnq.get_user_query(user_id_list))
    logging.debug("user_id_list of length %d -> token list of length %d" % 
        (len(user_id_list), len(token_list)))
    return __get_default_interface().send_silent_notification(token_list, json_data, dev)

def send_silent_notification_to_ios_with_interval(interval, dev=False):
    query = pnq.combine_queries([pnq.get_platform_query("ios"),
                                 pnq.get_sync_interval_query(interval)])
    token_list = pnq.get_matching_tokens(query)
    logging.debug("found %d tokens for ios with interval %d" % (len(token_list), interval))
    return __get_default_interface().send_silent_notification(token_list, {}, dev)

def display_response(response):
    if response is None:
        logging.debug("did not send push to push notification service")
        return

    return __get_default_interface().display_response(response)
