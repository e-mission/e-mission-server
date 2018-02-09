import emission.storage.timeseries.abstract_timeseries as esta
import pandas as pd
import requests
import json
import re
import emission.core.get_database as edb
from uuid import UUID
def calculate_single_suggestion(uuid):
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
        if cleaned_sections.iloc[i]["end_ts"] - cleaned_sections.iloc[i]["start_ts"] < 5 * 60:
            continue
        distance_in_miles = cleaned_sections.iloc[i]["distance"] * 0.000621371
        mode = cleaned_sections.iloc[i]["sensed_mode"]
        start_loc = cleaned_sections.iloc[i]["start_loc"]["coordinates"]
        trip_id = cleaned_sections.iloc[i]['trip_id']
        start_lat = str(start_loc[0])
        start_lon = str(start_loc[1])
        print(cleaned_sections.iloc[i]["trip_id"])
        print(cleaned_sections.iloc[i]["start_fmt_time"])
        end_loc = cleaned_sections.iloc[i]["end_loc"]["coordinates"]
        end_lat = str(end_loc[0])
        end_lon = str(end_loc[1])
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
def handle_insert(tripDict, tripID, collection, uuid):
    if tripDict == None:
        collection.insert_one({'uuid': uuid, 'trip_id': tripID})
        return False
    else:
        if tripDict['trip_id'] != tripID:
            return True
        else:
            return False
