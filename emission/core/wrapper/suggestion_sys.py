import emission.storage.timeseries.abstract_timeseries as esta
import pandas as pd
import requests
import json
import re
import emission.core.get_database as edb
from uuid import UUID
ACCESS_TOKEN = 'AIzaSyAbnpsty2SAzEX9s1VVIdh5pTHUPMjn3lQ' #GOOGLE MAPS ACCESS TOKEN
JACK_TOKEN = 'AIzaSyAXG_8bZvAAACChc26JC6SFzhuWysRqQPo'


def return_address_from_location(location='0,0'):
    """
    Creates a Google Maps API call that returns the addresss given a lat, lon
    """
    if not re.compile('^(\-?\d+(\.\d+)?),\s*(\-?\d+(\.\d+)?)$').match(location):
        raise ValueError('Location Invalid')
    base_url = 'https://maps.googleapis.com/maps/api/geocode/json?'
    latlng = 'latlng=' + location
    try:
        #This try block is for our first 150,000 requests. If we exceed this, use Jack's Token.
        key_string = '&key=' + ACCESS_TOKEN
        url = base_url + latlng + key_string #Builds the url
        result = requests.get(url).json() #Gets google maps json file
        cleaned = result['results'][0]['address_components']
        #Address to check against value of check_against_business_location
        chk = cleaned[0]['long_name'] + ' ' + cleaned[1]['long_name'] + ', ' + cleaned[3]['long_name']
        business_tuple = check_against_business_location(location, chk)
        if business_tuple[0]: #If true, the lat, lon matches a business location and we return business name
            return business_tuple[1]
        else: #otherwise, we just return the address
            return cleaned[0]['long_name'] + ' ' + cleaned[1]['short_name'] + ', ' + cleaned[3]['short_name']
    except:
        try:
            #Use Jack's Token in case of some invalid request problem with other API Token
            key_string = '&key=' + JACK_TOKEN
            url = base_url + latlng + key_string #Builds the url
            result = requests.get(url).json() #Gets google maps json file
            cleaned = result['results'][0]['address_components']
            #Address to check against value of check_against_business_location
            chk = cleaned[0]['long_name'] + ' ' + cleaned[1]['long_name'] + ', ' + cleaned[3]['long_name']
            business_tuple = check_against_business_location(location, chk)
            if business_tuple[0]: #If true, the lat, lon matches a business location and we return business name
                return business_tuple[1]
            else: #otherwise, we just return the address
                return cleaned[0]['long_name'] + ' ' + cleaned[1]['short_name'] + ', ' + cleaned[3]['short_name']
        except:
            raise ValueError("Something went wrong")

def check_against_business_location(location='0, 0', address = ''):
    if not re.compile('^(\-?\d+(\.\d+)?),\s*(\-?\d+(\.\d+)?)$').match(location):
        raise ValueError('Location Invalid')
    base_url = 'https://maps.googleapis.com/maps/api/place/nearbysearch/json?'
    location = 'location=' + location
    try:
        key_string = '&key=' + ACCESS_TOKEN
        radius = '&radius=10'
        url = base_url + location + radius + key_string
        result = requests.get(url).json()
        cleaned = result['results']
        for i in cleaned:
            #If the street address matches the street address of this business, we return a tuple
            #signifying success and the business name
            if address == i['vicinity']:
                return (True, i['name'])
        else:
            return (False, '')
    except:
        try:
            key_string = '&key=' + JACK_TOKEN
            radius = '&radius=10'
            url = base_url + location + radius + key_string
            result = requests.get(url).json()
            cleaned = result['results']
            for i in cleaned:
                if address == i['vicinity']:
                    return (True, i['name'])
            else:
                return (False, '')
        except:
            raise ValueError("Something went wrong")

def calculate_single_suggestion(uuid):
    #Given a single UUID, create a suggestion for them
    return_obj = { 'message': "Good job walking and biking! No suggestion to show.",
    'savings': "0", 'start_lat' : '0.0', 'start_lon' : '0.0',
    'end_lat' : '0.0', 'end_lon' : '0.0', 'method' : 'bike'}
    all_users = pd.DataFrame(list(edb.get_uuid_db().find({}, {"uuid": 1, "_id": 0})))
    user_id = all_users.iloc[all_users[all_users.uuid == uuid].index.tolist()[0]].uuid
    time_series = esta.TimeSeries.get_time_series(user_id)
    cleaned_sections = time_series.get_data_df("analysis/cleaned_section", time_query = None)
    suggestion_trips = edb.get_suggestion_trips_db()
    #Go in reverse order because we check by most recent trip
    counter = 40
    if len(cleaned_sections) == 0:
        return_obj['message'] = 'Suggestions will appear once you start taking trips!'
    return return_obj
    for i in range(len(cleaned_sections) - 1, -1, -1):
        counter -= 1
        if counter < 0:
            #Iterate 20 trips back
            return return_obj
        if cleaned_sections.iloc[i]["end_ts"] - cleaned_sections.iloc[i]["start_ts"] < 5 * 60:
            continue
        distance_in_miles = cleaned_sections.iloc[i]["distance"] * 0.000621371
        mode = cleaned_sections.iloc[i]["sensed_mode"]
        start_loc = cleaned_sections.iloc[i]["start_loc"]["coordinates"]
        start_lat = str(start_loc[0])
        start_lon = str(start_loc[1])
        trip_id = cleaned_sections.iloc[i]['trip_id']
        tripDict = suggestion_trips.find_one({'uuid': uuid})
        print(tripDict)
        end_loc = cleaned_sections.iloc[i]["end_loc"]["coordinates"]
        end_lat = str(end_loc[0])
        end_lon = str(end_loc[1])
        #TODO: Add elif's for bus
        if mode == 0 and distance_in_miles >= 5 and distance_in_miles <= 15:
            #Suggest bus if it is car and distance between 5 and 15
            default_message = return_obj['message']
            try:
                message = "Try public transportation from " + return_address_from_location(start_lon + "," + start_lat) + \
                " to " + return_address_from_location(end_lon + "," + end_lat)
                #savings per month, .465 kg co2/mile for car, 0.14323126 kg co2/mile for bus
                savings = str(int(distance_in_miles * 30 * .465 - 0.14323126 * distance_in_miles * 30))
                return {'message' : message, 'savings' : savings, 'start_lat' : start_lat,
                'start_lon' : start_lon, 'end_lat' : end_lat, 'end_lon' : end_lon, 'method': 'public'}
                insert_into_db(tripDict, trip_id, suggestion_trips, uuid)
                break
            except ValueError as e:
                return_obj['message'] = default_message
                continue
        elif mode == 0 and distance_in_miles < 5 and distance_in_miles >= 1:
            #Suggest bike if it is car/bus and distance between 5 and 1
            try:
                message = "Try biking from " + return_address_from_location(start_lon + "," + start_lat) + \
                " to " + return_address_from_location(end_lon + "," + end_lat)
                savings = str(int(distance_in_miles * 30 * .465))  #savings per month, .465 kg co2/mile
                insert_into_db(tripDict, trip_id, suggestion_trips, uuid)
                return {'message' : message, 'savings' : savings, 'start_lat' : start_lat,
                'start_lon' : start_lon, 'end_lat' : end_lat, 'end_lon' : end_lon, 'method': 'bike'}
                break
            except:
                continue
        elif mode == 0 and distance_in_miles < 1:
            #Suggest walking if it is car/bus and distance less than 1
            try:
                message = "Try walking/biking from " + return_address_from_location(start_lon + "," + start_lat) + \
                " to " + return_address_from_location(end_lon + "," + end_lat)
                savings = str(int(distance_in_miles * 30 * .465)) #savings per month, .465 kg co2/mile
                insert_into_db(tripDict, trip_id, suggestion_trips, uuid)
                return {'message' : message, 'savings' : savings, 'start_lat' : start_lat,
                'start_lon' : start_lon, 'end_lat' : end_lat, 'end_lon' : end_lon, 'method': 'walk'}
                break
            except:
                continue
    return return_obj
def insert_into_db(tripDict, tripID, collection, uuid):
    print(tripDict)
    if tripDict == None:
        collection.insert_one({'uuid': uuid, 'trip_id': tripID})
    else:
        if tripDict['trip_id'] != tripID:
            collection.update_one({'uuid': uuid}, {'$set':{'trip_id' : tripID}})
