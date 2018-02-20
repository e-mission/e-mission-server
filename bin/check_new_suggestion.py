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
            return True
        else:
            return False

def calculate_single_suggestion(uuid):
    logging.debug("About to calculate single suggestion for %s" % uuid)
    #Given a single UUID, create a suggestion for them
    suggestion_trips = edb.get_suggestion_trips_db()
    return_obj = { 'message': "Good job walking and biking! No suggestion to show.",
    'savings': "0", 'start_lat' : '0.0', 'start_lon' : '0.0',
    'end_lat' : '0.0', 'end_lon' : '0.0', 'method' : 'bike'}
    all_users = pd.DataFrame(list(edb.get_uuid_db().find({}, {"uuid": 1, "_id": 0})))
    user_id = all_users.iloc[all_users[all_users.uuid == uuid].index.tolist()[0]].uuid
    time_series = esta.TimeSeries.get_time_series(user_id)
    cleaned_sections = time_series.get_data_df("analysis/cleaned_section", time_query = None)
    #Go in reverse order because we check by most recent trip
    counter = 40
    for i in range(len(cleaned_sections) - 1, -1, -1):
        counter -= 1
        if counter < 0:
            #Iterate 20 trips back
            return False
        logging.debug("Considering section from %s -> %s" %
            (cleaned_sections.iloc[i]["start_fmt_time"],
             cleaned_sections.iloc[i]["end_fmt_time"]))
        if cleaned_sections.iloc[i]["end_ts"] - cleaned_sections.iloc[i]["start_ts"] < 5 * 60:
            continue
        trip_id = cleaned_sections.iloc[i]['trip_id']
        tripDict = suggestion_trips.find_one({'uuid': uuid})

        #TODO: Add elif's for bus
        if mode == 0 and distance_in_miles >= 5 and distance_in_miles <= 15:
            #Suggest bus if it is car and distance between 5 and 15
            return handle_insert(tripDict, trip_id, suggestion_trips, uuid)
        elif mode == 0 and distance_in_miles < 5 and distance_in_miles >= 1:
            #Suggest bike if it is car/bus and distance between 5 and 1
            #TODO: Change ret_boj and figure out how to change lat and lon to places
            return handle_insert(tripDict, trip_id, suggestion_trips, uuid)
        elif mode == 0 and distance_in_miles < 1:
            #Suggest walking if it is car/bus and distance less than 1
            return handle_insert(tripDict, trip_id, suggestion_trips, uuid)
    return False

def push_to_user(uuid_list, message):
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
            uuid = all_users[i].uuid
            client = all_users[i].client
            if client == "urap-2017-emotion":
                if pb.getMoodChange(uuid):
                    happiness_uuids.append(uuid)
            elif client == "urap-2017-information":
                if calculate_single_suggestion(uuid):
                    suggestion_uuids.append(uuid)
        except:
            continue
    push_to_user(suggestion_uuids, "You have a new suggestion! Tap me to see it.")
    push_to_user(happiness_uuids, "Your polar bear's mood has changed since yesterday! Tap me to see it.")

if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    print("Set log leve to DEBUG")
    check_all_suggestions()
