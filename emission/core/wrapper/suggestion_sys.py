from __future__ import print_function
from datetime import datetime
from uuid import UUID
import pandas as pd
import requests
import json
import logging
import re

import emission.storage.timeseries.abstract_timeseries as esta
import argparse
import pprint
import requests
import os
import emission.net.ext_service.geocoder.nominatim as geo
import bson

# Yelp Fusion no longer uses OAuth as of December 7, 2017.
# You no longer need to provide Client ID to fetch Data
# It now uses private keys to authenticate requests (API Key)
# You can find it on
# https://www.yelp.com/developers/v3/manage_app

#RESTRUCTURE CODE FOR GOOGLE MAPS SO CAN GET RID OF IT AND JUST USE NOMINATIM.PY
yelp_json_path = 'conf/net/ext_service/yelpfusion.json'

"""
Checks if conf files exists or not. The conf files will be given to the user through request.
"""

try:
    yelp_json = open('conf/net/ext_service/yelpfusion.json', 'r')
    yelp_auth = json.load(yelp_json)
except:
    print("yelp not configured, cannot generate suggestions")


YELP_API_KEY = yelp_auth['api_key']
MAPQUEST_KEY = yelp_auth['map_quest_key']
API_HOST = yelp_auth['api_host']
SEARCH_PATH = yelp_auth['search_path']
BUSINESS_PATH = yelp_auth['business_path']
SEARCH_LIMIT = yelp_auth['search_limit']

ZIPCODE_API_KEY = yelp_auth['zip_code_key']
ZIP_HOST_URL = yelp_auth['zip_code_host']
ZIP_FORMAT = yelp_auth['zip_code_format']
ZIP_DEGREE = yelp_auth['zip_code_degree']
BACKUP_ZIP_KEY = yelp_auth['backup_zip_code_key']


"""
YELP API: Helper function to query into the API domain.
"""
def request(host, path, api_key, url_params=None):
    """Given your API_KEY, send a GET request to the API.
    Args:
        host (str): The domain host of the API.
        path (str): The path of the API after the domain.
        API_KEY (str): Your API Key.
        url_params (dict): An optional set of query parameters in the request.
    Returns:
        dict: The JSON response from the request.
    Raises:
        HTTPError: An error occurs from the HTTP request.
    """
    url_params = url_params or {}
    url = '{0}{1}'.format(host, path)
    headers = {
        'Authorization': 'Bearer %s' % api_key,
    }

    # print('Querying {0} ...'.format(url))

    response = requests.request('GET', url, headers=headers, params=url_params)
    return response.json()

"""
YELP API: Function to query based on search terms
"""
def search(api_key, term, location):
    """Query the Search API by a search term and location.
    Args:
        term (str): The search term passed to the API.
        location (str): The search location passed to the API.
    Returns:
        dict: The JSON response from the request.
    """

    url_params = {
        'term': term.replace(' ', '+'),
        'location': location.replace(' ', '+'),
        'limit': SEARCH_LIMIT
    }
    return request(API_HOST, SEARCH_PATH, api_key, url_params=url_params)

def lat_lon_search(api_key, lat, lon, radius):
    """Query Search API using latitude and longitude.
    Args:
        lat (float) : latitude
        lon (float) : longitude
        radius (int) : radius of search in meters
    Returns:
        dict: The JSON response form the request.
    """
    url_params = {
        'latitude': lat,
        'longitude': lon,
        'radius' : radius,
        'limit': SEARCH_LIMIT,
        'sort_by': 'distance',
        'categories' : 'food,restaurants,shopping,hotels,beautysvc,auto,education,collegeuniv,financialservices,publicservicesgovt'
    }
    return request(API_HOST, SEARCH_PATH, api_key, url_params=url_params)

"""
YELP API: Function to retrieve details of the business with the specified id
"""
def business_details(api_key, business_id):
    business_path = BUSINESS_PATH + business_id

    return request(API_HOST, business_path, api_key)

"""
NOMINATIM API: Creates a Nominatim API Call, returns address in string form and dictionary form separated by streetname,
    road, neighborhood, etc
"""
def return_address_from_location_nominatim(lat, lon):
    geocode_obj = geo.Geocoder()
    return geocode_obj.reverse_geocode(lat, lon)

'''
GOOGLE API: Makes Google Maps API CALL to the domain and returns address given a latitude and longitude
'''

def return_address_from_google_nomfile(lat, lon):
    return geo.return_address_from_location_google(lat, lon)


'''
YELP API: Function to find the business matching the address
'''
def match_business_address(address):
    business_path = SEARCH_PATH
    url_params = {
        'location': address.replace(' ', '+')
    }
    return request(API_HOST, business_path, YELP_API_KEY, url_params)

"""
NOMINATIM API: Checks if location given by nominatim call is a residential location

"""
def is_service_nominatim(business):
    if "Hall" in business:
        return False
    return True

'''
NOMINATIM VERS: Function that RETURNS a list of categories that the business falls into

Using the Google reverse lookup in the except clause, in case Nominatim's results are too vague.
Will first try Nominatim's reverse lookup, but if Nominatim returns a broad "address"
of the street and the city, without a full address with a specific location
Such as Piedmont Ave, Berkeley, CA

Then the function will enter the Google reverse lookup and choose a business that is closest to
latitude and longitude given
'''


### BEGIN: Pulled out candidate functions so that we can evaluate individual accuracies
def find_destination_business_google(lat, lon):
    return return_address_from_google_nomfile(lat, lon)

def find_destination_business_yelp(lat, lon):
    yelp_from_lat_lon = lat_lon_search(YELP_API_KEY, lat, lon, 250)
    if yelp_from_lat_lon == {}:
        return (None, None, None, False)
    businesses = yelp_from_lat_lon['businesses']
    if businesses == []:
        return find_destination_business(lat, lon)
    business_name = businesses[0]['name']
    address = businesses[0]['location']['address1']
    city = businesses[0]['location']['city']
    #If there is no commercial establishment in a 50 meter (1/2 block) radius of coordinate
    #It is safe to assume the area is not a commercial establishment
    location_is_service = True
    print((business_name, address, city, location_is_service))
    return (business_name, address, city, location_is_service)

def find_destination_business_nominatim(lat, lon):
    string_address, address_dict = return_address_from_location_nominatim(lat, lon)
    business_key = list(address_dict.keys())[0]
    business_name = address_dict[business_key]
    city = get_city_from_address(address_dict)
    if city is None:
        city = ''
    return (business_name, string_address, city,
        (not is_service_nominatim(business_name)))
### END: Pulled out candidate functions so that we can evaluate individual accuracies

## Current combination of candidate functions;
## First try nominatim, and if it fails, fall back to google
## Is that the right approach?

def find_destination_business(lat, lon):
    #Off at times if the latlons are of a location that takes up a small spot, especially boba shops
    # print(return_address_from_location_google(location))
    # print(len(return_address_from_location_google(location)))
    #IF RETURN_ADDRESS_FROM_LOCATION HAS A BUSINESS LOCATION ATTACHED TO THE ADDRESS
    try:
        return_tuple = find_destination_business_nominatim(lat, lon)
        logging.debug("Nominatim found destination business %s " % str(return_tuple))
        return return_tuple
    except:
        #USE GOOGLE API JUST IN CASE if nominatim doesn't work
        return_tuple = return_address_from_google_nomfile(lat, lon)
        logging.debug("Nominatim failed, Google found destination business %s "
            % str(return_tuple))
        return return_tuple


'''
Function that RETURNS distance between lat,lng pairs
'''
def distance(start_lat, start_lon, end_lat, end_lon):
    start_lat_lon = start_lat + "," + start_lon
    end_lat_lon = end_lat + "," + end_lon

    url = 'http://www.mapquestapi.com/directions/v2/route?key=' + MAPQUEST_KEY + '&from=' + start_lat_lon + '&to=' + end_lat_lon
    response = requests.get(url)
    return response.json()['route']['distance']
    # except:
    #     url = 'http://www.mapquestapi.com/directions/v2/route?key=' + BACKUP_MAPQUEST_KEY + '&from=' + address1 + '&to=' + address2
    #     response = requests.get(url)
    #     print(response.json())
    #     return response.json()['route']['distance']


'''
Two functions that RETURN latitude and longitude coordinates from GEOJSON file
'''
def geojson_to_latlon(geojson):
    lat, lon = geojson_to_lat_lon_separated(geojson)
    lat_lon = lat + ',' + lon
    return lat_lon

def geojson_to_lat_lon_separated(geojson):
    coordinates = geojson["coordinates"]
    lon = str(coordinates[0])
    lat = str(coordinates[1])
    return lat, lon

'''
REWRITE def check_mode_from_trip(cleaned_trip, cleaned_sections, section_counter, trip_counter):
Mode number correspondence:
0: "IN_VEHICLE"
1: "BIKING"
2: "ON_FOOT"
3: "STILL"
4: "UNKNOWN"
5: "TILTING"
7: "WALKING"
8: "RUNNING"
9: "NONE"
10: "STOPPED_WHILE_IN_VEHICLE"
11: "AIR_ON_HSR"
'''
def check_mode_from_trip(cleaned_trip, cleaned_sections, section_counter, trip_counter):
    end_location = cleaned_trip.iloc[trip_counter]["end_loc"]
    end_loc_lat, end_loc_lon = geojson_to_lat_lon_separated(end_location)
    modes_from_section = []
    endsec_location = cleaned_sections.iloc[section_counter]["end_loc"]
    endsec_loc_lat, endsec_loc_lon = geojson_to_lat_lon_separated(end_sec_location)
    # Trash value for mode
    mode = -10
    if (endsec_loc_lat == end_loc_lat and endsec_loc_lon == end_loc_lon):
        mode = cleaned_sections.iloc[section_counter]["sensed_mode"]
        return mode, section_counter + 1
    while endsec_loc_lat != end_loc_lat and endsec_loc_lon != end_loc_lon and section_counter < len(cleaned_sections) :
        mode = cleaned_sections.iloc[section_counter]["sensed_mode"]
        modes_from_section.append(mode)
        endsec_location = cleaned_sections.iloc[section_counter]["end_loc"]
        endsec_loc_lon, endsec_loc_lon = geojson_to_lat_lon_separated(endsec_location)
        if (mode == 0):
            return mode, section_counter+1
    return mode, section_counter + 1


'''
DUMMY HELPER FUNCTION TO TEST if server and phone side are connected
'''

def dummy_starter_suggestion(uuid):
    user_id = uuid
    time_series = esta.TimeSeries.get_time_series(user_id)
    cleaned_sections = time_series.get_data_df("analysis/cleaned_trip", time_query = None)
    real_cleaned_sections = time_series.get_data_df("analysis/inferred_section", time_query = None)
    modes_from_trips = {}
    section_counter = 0
    for i in range(len(cleaned_sections)):
        modes_from_trips[i], section_counter = most_used_mode_from_trip(cleaned_sections, real_cleaned_sections, section_counter, i)
    return modes_from_trips

'''
ZIPCODEAPI

As nominatim sometimes is unable to provide a specific location with the city and instead returns
a postcode (zipcode) and the country name. For the suggestions that we built, the suggestions
require which city (city name) it is in order to look for other similar categoried services
in the area. Thus, this function takes in the INPUT of a zipcode, and RETURNS the name of the city.

'''
def zipcode_to_city(zipcode):
    # Use this API key first.
    url = ZIP_HOST_URL + ZIPCODE_API_KEY + ZIP_FORMAT + zipcode + ZIP_DEGREE
    response = requests.request('GET', url=url)
    results = response.json()

    if "error_code" in results:
        # In case the first API key runs out of requests per hour.
        url = ZIP_HOST_URL + BACKUP_ZIP_KEY + ZIP_FORMAT + zipcode + ZIP_DEGREE
        response = requests.request('GET', url=url)
        response_json = response.json()
        return response_json["city"]
    else:
        return None

def get_city_from_address(address_dict):
    if "city" in address_dict:
        return address_dict["city"]
    if "town" in address_dict:
        return address_dict["town"]

    # Falling back to zipcode
    zipcode = address_dict["postcode"]
    city = zipcode_to_city(zipcode)
    # Note that `zipcode_to_city` returns None if the result is not json
    return city

'''
NOMINATIM
In progress-nominatim yelp server suggestion function, first just trying to make end-to-end work before robustifying this function.

Mode number correspondence:
0: "IN_VEHICLE"
1: "BIKING"
2: "ON_FOOT"
3: "STILL"
4: "UNKNOWN"
5: "TILTING"
7: "WALKING"
8: "RUNNING"
9: "NONE"
10: "STOPPED_WHILE_IN_VEHICLE"
11: "AIR_ON_HSR"
'''

def calculate_yelp_server_suggestion_singletrip_nominatim(uuid, tripidstr):
    user_id = uuid
    tripid = bson.objectid.ObjectId(tripidstr)
    timeseries = esta.TimeSeries.get_time_series(user_id)
    cleaned_trips = timeseries.get_entry_from_id("analysis/cleaned_trip", tripid)
    '''
    Used the abstract time series method, wanted to make sure this was what you were asking for
    '''
    start_location = cleaned_trips.data.start_loc
    end_location = cleaned_trips.data.end_loc
    '''
    Distance in miles because the current calculated distances is through MapQuest which uses miles,
    still working on changing those functions, because haven't found any functions through nominatim
    that calculates distance between points.
    '''
    suggestion_result = calculate_yelp_server_suggestion_for_locations(start_location, end_location, cleaned_trips.data.distance)
    # we could fill in the tripid here as well since we know it, but we weren't doing it before,
    # so let's not mess it up
    return suggestion_result

def calculate_yelp_server_suggestion_for_locations(start_location, end_location, distance):
    distance_in_miles = distance * 0.000621371
    start_lat, start_lon = geojson_to_lat_lon_separated(start_location)
    end_lat, end_lon = geojson_to_lat_lon_separated(end_location)

    orig_end_business_details = find_destination_business(end_lat, end_lon)
    logging.debug("orig_end_business_details = %s " % str(orig_end_business_details))
    if not orig_end_business_details[-1]:
        # This is not a service, so we bail right now
        return format_suggestion(start_lat, start_lon, None, None, 'bike')
    business_name = orig_end_business_details[0]
    city = orig_end_business_details[2]
    orig_end_bid_hack = business_name.replace(' ', '-') + '-' + city
    orig_bus_details = business_details(YELP_API_KEY, orig_end_bid_hack)

    alt_sugg_list = get_potential_suggestions(orig_bus_details)
    fill_distances(start_lat, start_lon, alt_sugg_list)
    final_bdetails, final_mode = get_selected_suggestion_and_mode(alt_sugg_list)

    return format_suggestion(start_lat, start_lon, orig_bus_details,
                             final_bdetails, final_mode)

#
# Returns a list of potential suggestions. Each entry is a {"bdetails":
# business_details_obj} map. We do this to make it easier to add on other calculated
# state (e.g. new distance, ...) later.
#

def get_potential_suggestions(orig_bus_details):
    logging.info("Finding potential suggestions for %s with categories %s" %
        (orig_bus_details['name'], orig_bus_details['categories']))
    endpoint_categories = [c['alias'] for c in orig_bus_details['categories']]
    orig_city = orig_bus_details['location']['city']
    orig_end_rating = orig_bus_details['rating']

    suggestion_list = []
    try:
        for categor in endpoint_categories:
            queried_bus = search(YELP_API_KEY, categor, city)['businesses']
            for q in queried_bus:
                if q['rating'] >= orig_end_rating:
                    suggestion_list.append({"bdetails": q})
    except Exception as e:
        logging.info("Found error %s while looking up suggestions for bid %s, returning empty" % (e.message, orig_end_bid))

    # no matter what happens above, we return the suggestion_list
    return suggestion_list

# Non functional programming;
# fills distances into existing object

def fill_distances(start_lat, start_lon, sugg_list):
    for sugg_obj in sugg_list:
        curr_sugg_details = sugg_obj["bdetails"]
        try:
            alt_distance = distance(start_lat, start_lon,
                curr_sugg_details["coordinates"][0], curr_sugg_details["coordinates"][1])
            logging.debug("While considering %s, calculated new distance %s" %
                (sugg_obj["alias"], alt_distance))
        except Exception as e:
            logging.info("Error %s while calculating distance for %s,returning inf" %
                (e, curr_sugg_details["alias"]))
            alt_distance = float('inf')

        sugg_obj["alt_distance"] = alt_distance

#
# sugg_list is the list of alternatives
# distance_in_miles is the distance of the original trip
# Every single check in here currently checks for calculate_distance <
# distance_in_miles so theoretically, we could introduce a separate filter step
# before this and simplify this function even further. But since
# `get_selected_suggestion_and_mode` is under active development, I will leave
# it unchanged
# Don't need any try/catch blocks here because we have them in the preceding functions

def get_selected_suggestion_and_mode(sugg_list, distance_in_miles):
    for sugg_obj in sugg_list:
        calculate_distance = sugg_obj["alt_distance"]
        #Will check which mode the trip was taking for the integrated calculate yelp suggestion
        if calculate_distance < distance_in_miles and calculate_distance < 5 and calculate_distance >= 1:
            return (sugg_obj, 'bike')
        elif calculate_distance < distance_in_miles and calculate_distance < 1:
            return (sugg_obj, 'walk')
        elif calculate_distance < distance_in_miles and calculate_distance >= 5 and calculate_distance <= 15:
            return (sugg_obj, 'public')
    return (None, 'public')

def format_suggestion(start_lat, start_lon, orig_bus_details,
                      alt_bus_details, alt_mode):
    if alt_bus_details is None:
        return {
            'message': 'Sorry, unable to retrieve datapoint because datapoint is a house or datapoint does not belong in service categories',
            'question': None,
            'suggested_loc': None,
            'method': 'bike',
            'rating': None,
            'businessid': None
        }
    else:
        begin_string_address, begin_address_dict = return_address_from_location_nominatim(start_lat, start_lon)
        # TODO: Can't we just use the business name here directly instead of an
        # address. Seems like that will be a lot more meaningful to people
        end_string_address, end_address_dict = return_address_from_location_nominatim(end_lat, end_lon)
        return {
            'message': 'We saw that you took a vehicle from '+begin_string_address
                + ' to '+ end_string_address,
            'suggested_loc': 'Instead, there is '+ alt_bus_details['name']+' which has better reviews and is closer to your starting point',
            'method': alt_mode,
            'rating': alt_bus_details['rating'],
            'businessid': alt_bus_details['alias']
        }

def calculate_yelp_server_suggestion_nominatim(uuid):
    user_id = uuid
    time_series = esta.TimeSeries.get_time_series(user_id)
    cleaned_trips = time_series.get_data_df("analysis/cleaned_trip", time_query = None)
    real_cleaned_sections = time_series.get_data_df("analysis/inferred_section", time_query = None)
    # modes_from_trips = {}
    section_counter = 0
    # for i in range(len(cleaned_trips)):
    #     modes_from_trips[i], section_counter = check_mode_from_trip(cleaned_trips, cleaned_sections, section_counter, i)
    if len(cleaned_trips) == 0:
        return_obj['message'] = 'Suggestions will appear once you start taking trips!'
        return return_obj
    for i in range(len(cleaned_trips) - 1, -1, -1):
        distance_in_miles = cleaned_trips.iloc[i]["distance"] * 0.000621371
        # mode = modes_from_trips[i]
        start_lat, start_lon = geojson_to_lat_lon_separated(cleaned_trips.iloc[i]["start_loc"])
        end_lat, end_lon = geojson_to_lat_lon_separated(cleaned_trips.iloc[i]["end_loc"])
        suggestion_result = calculate_yelp_server_suggestion_for_locations(cleaned_trips.iloc[i]["start_loc"], cleaned_trips.iloc[i]["end_loc"], cleaned_trips.iloc[i]["distance"])
        suggestion_result['tripid'] = cleaned_trips.iloc[i]["_id"]
        return suggestion_result
