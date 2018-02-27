from __future__ import unicode_literals
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import
import emission.storage.timeseries.abstract_timeseries as esta
import emission.net.ext_service.push.notify_usage as pnu
import emission.core.wrapper.user as ecwu
from future import standard_library
standard_library.install_aliases()
from builtins import *
import logging
import logging.config
import argparse
import pandas as pd
import requests
import json
import re
import emission.core.get_database as edb
from uuid import UUID
import emission.core.wrapper.polarbear as pb

def handle_insert(tripDict, tripID, collection, uuid):
    if tripDict == None:
        collection.insert_one({'uuid': uuid, 'trip_id': tripID})
        return True
    else:
        if tripDict['trip_id'] != tripID:
            collection.update_one({'uuid': uuid}, {'$set': {'trip_id' : tripID}})
            return True
        else:
            return False

def calculate_single_suggestion(uuid):
    logging.debug("About to calculate single suggestion for %s" % uuid)
    all_users = pd.DataFrame(list(edb.get_uuid_db().find({}, {"uuid": 1, "_id": 0})))
    #Given a single UUID, create a suggestion for them
    suggestion_trips = edb.get_suggestion_trips_db()
    return_obj = { 'message': "Good job walking and biking! No suggestion to show.",
    'savings': "0", 'start_lat' : '0.0', 'start_lon' : '0.0',
    'end_lat' : '0.0', 'end_lon' : '0.0', 'method' : 'bike'}
    user_id = all_users.iloc[all_users[all_users.uuid == uuid].index.tolist()[0]].uuid
    time_series = esta.TimeSeries.get_time_series(user_id)
    cleaned_sections = time_series.get_data_df("analysis/cleaned_section", time_query = None)
    cleaned_sections = time_series.get_data_df("analysis/cleaned_section", time_query = None)
    #Go in reverse order because we check by most recent trip
    counter = 40
    for i in range(len(cleaned_sections) - 1, -1, -1):
        counter -= 1
        if counter < 0:
            #Iterate 20 trips back
            return False
        if cleaned_sections.iloc[i]["end_ts"] - cleaned_sections.iloc[i]["start_ts"] < 5 * 60:
            continue
        distance_in_miles = cleaned_sections.iloc[i]["distance"] * 0.000621371
        trip_id = cleaned_sections.iloc[i]['trip_id']
        tripDict = suggestion_trips.find_one({'uuid': uuid})
        logging.debug("%s" % tripDict)
        mode = cleaned_sections.iloc[i]["sensed_mode"]
        if mode == 5 and distance_in_miles >= 5 and distance_in_miles <= 15:
            logging.debug("Considering section from %s -> %s" %
                (cleaned_sections.iloc[i]["start_fmt_time"],
                 cleaned_sections.iloc[i]["end_fmt_time"]))
            #Suggest bus if it is car and distance between 5 and 15
            return handle_insert(tripDict, trip_id, suggestion_trips, uuid)
        elif mode == 5 or mode == 3 or mode == 4 and distance_in_miles < 5 and distance_in_miles >= 1:
            logging.debug("Considering section from %s -> %s" %
                (cleaned_sections.iloc[i]["start_fmt_time"],
                 cleaned_sections.iloc[i]["end_fmt_time"]))
            #Suggest bike if it is car/bus and distance between 5 and 1
            #TODO: Change ret_boj and figure out how to change lat and lon to places
            return handle_insert(tripDict, trip_id, suggestion_trips, uuid)
            logging.debug("Considering section from %s -> %s" %
                (cleaned_sections.iloc[i]["start_fmt_time"],
                 cleaned_sections.iloc[i]["end_fmt_time"]))
        elif mode == 5 or mode == 3 or mode == 4 and distance_in_miles < 1:
            logging.debug("Considering section from %s -> %s" %
                (cleaned_sections.iloc[i]["start_fmt_time"],
                 cleaned_sections.iloc[i]["end_fmt_time"]))
            #Suggest walking if it is car/bus and distance less than 1
            return handle_insert(tripDict, trip_id, suggestion_trips, uuid)
    return False

def push_to_user(uuid_list, message):
    logging.debug("About to send notifications to: %s users" % len(uuid_list))
    json_data = {
        "title": "TripAware Notification",
        "message": message
    }
    logging.debug(uuid_list)
    response = pnu.send_visible_notification_to_users(uuid_list,
                                                        json_data["title"],
                                                        json_data["message"],
                                                        json_data,
                                                        dev = False)
    pnu.display_response(response)

def check_all_suggestions():
    suggestion_uuids = []
    happiness_uuids = []
    all_users = pd.DataFrame(list(edb.get_uuid_db().find({}, {"user_email":1, "uuid": 1, "_id": 0})))
    logging.debug("About to iterate over %s users" % len(all_users))
    for i in range(len(all_users)):
        try:
            curr_uuid = all_users.iloc[i].uuid
            client = edb.get_profile_db().find_one({"user_id": curr_uuid})['client']
            if client == "urap-2017-emotion":
                if pb.getMoodChange(curr_uuid):
                    happiness_uuids.append(curr_uuid)
            elif client == "urap-2017-information":
                if calculate_single_suggestion(curr_uuid):
                    suggestion_uuids.append(curr_uuid)
        except:
            logging.debug("error on %s" % all_users.iloc[i].user_email)
            continue
    push_to_user(suggestion_uuids, "You have a new suggestion! Tap me to see it.")
    push_to_user(happiness_uuids, "Your polar bear's mood has changed since yesterday! Tap me to see it.")

if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    print("Set log level to DEBUG")
    check_all_suggestions()
