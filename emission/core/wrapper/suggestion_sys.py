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

try:
    # For Python 3.0 and later
    from urllib.error import HTTPError
    from urllib.parse import quote
    from urllib.parse import urlencode
except ImportError:
    # Fall back to Python 2's urllib2 and urllib
    from urllib2 import HTTPError
    from urllib import quote
    from urllib import urlencode


# Yelp Fusion no longer uses OAuth as of December 7, 2017.
# You no longer need to provide Client ID to fetch Data
# It now uses private keys to authenticate requests (API Key)
# You can find it on
# https://www.yelp.com/developers/v3/manage_app

#RESTRUCTURE CODE FOR GOOGLE MAPS SO CAN GET RID OF IT AND JUST USE NOMINATIM.PY
yelp_json_path = 'conf/net/ext_service/yelpfusion.json'
nominatim_path = 'conf/net/ext_service/nominatim.json'
"""
Checks if conf files exists or not. The conf files will be given to the user through request.
"""

try:
    yelp_json = open('conf/net/ext_service/yelpfusion.json', 'r')
    yelp_auth = json.load(yelp_json)
except:
    print("nominatim not configured either, place decoding must happen on the client")


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
    url = '{0}{1}'.format(host, quote(path.encode('utf8')))
    headers = {
        'Authorization': 'Bearer %s' % api_key,
    }

    # print(u'Querying {0} ...'.format(url))

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
YELP API: Function to retrieve all reviews related to the business.
"""
def business_reviews(api_key, business_id):
    business_path = BUSINESS_PATH + business_id

    return request(API_HOST, business_path, api_key)

"""
YELP API: Returns the title of the business category in the API call
"""
def title_of_category(json_file):
    return json_file["categories"][0]["title"]

"""
YELP API: Obtains the business ID through latitude, longitude
"""
def get_business_id(api_key, lat, lon):
    url_params = {
        'location': lat + ',' + lon
    }
    return request(API_HOST, SEARCH_PATH, api_key, url_params=url_params)

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
    try:
        city = address_dict['city']
    except:
        try:
            city = address_dict["town"]
        except:
            try:
                zipcode = address_dict["postcode"]
                city = zipcode_to_city(zipcode)
            except:
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


### BEGIN: Pulled out candidate functions so that we can evaluate individual accuracies
def category_of_business_awesome(lat, lon):
    return []

def category_from_name(business_name):
    categories = []
    for c in business_reviews(YELP_API_KEY, business_name.replace(' ', '-') + '-' + city)['categories']:
        categories.append(c['alias'])
    return categories

def category_from_address(address):
    categories = []
    possible_bus = match_business_address(address)["businesses"][0]
    possible_categ = possible_bus["categories"]
    for p in possible_categ:
        categories.append(p["alias"])
    return categories
### END: Pulled out candidate functions so that we can evaluate individual accuracies

### BEGIN: Wrappers for the candidate functions to make them callable from the harness
### We do not want to use them in the combination function directly because that will
### result in two separate calls to `find_destination_business`
def category_from_name_wrapper(lat, lon):
    business_name, address, city, location_is_service = find_destination_business(lat, lon)
    if not location_is_service:
        return []

    if business_name is None:
        return []

    return category_from_name(business_name)

def category_from_address_wrapper(lat, lon):
    business_name, address, city, location_is_service = find_destination_business(lat, lon)
    if not location_is_service:
        return []

    return category_from_address(address)
### END: Wrappers for the candidate functions to make them callable from the harness

## Current combination of candidate functions;
## First try name, and if it fails, fall back to address
## Is that the right approach?
def category_of_business_nominatim(lat, lon):
    try:
        business_name, address, city, location_is_service = find_destination_business(lat, lon)
        if not location_is_service:
            return []

        categories = []
        if business_name is not None:
            return category_from_name(business_name)
        else:
            return category_from_address(address)
    except:
        raise ValueError("Something went wrong")


'''
Function that RETURNS distance between addresses
'''
def distance(address1, address2):
    address1 = address1.replace(' ', '+')
    address2 = address2.replace(' ', '+')


    url = 'http://www.mapquestapi.com/directions/v2/route?key=' + MAPQUEST_KEY + '&from=' + address1 + '&to=' + address2
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
NOMINATIM VERSION: Function to find the review of the original location of the end point of a trip
'''
def review_start_loc_nominatim(lat, lon):
    try:
        #Off at times if the latlons are of a location that takes up a small spot, especially boba shops

        #IF RETURN_ADDRESS_FROM_LOCATION HAS A BUSINESS LOCATION ATTACHED TO THE ADDRESS
        if (len(return_address_from_location_nominatim(lat, lon)) == 2):
            address, address_dict = return_address_from_location_nominatim(lat, lon)
            business_name = address_dict[list(address_dict.keys())[0]]
            city = address_dict['city']
        #print(business_reviews(API_KEY, business_name.replace(' ', '-') + '-' + city))
        return business_reviews(YELP_API_KEY, business_name.replace(' ', '-') + '-' + city)['rating']
    except:
        try:
            #This EXCEPT part may error, because it grabs a list of businesses instead of matching the address to a business
            address, address_dict = return_address_from_location_nominatim(lat, lon)
            possible_bus = match_business_address(address)["businesses"][0]
            possible_review = possible_bus["rating"]
            return possible_review
        except:
            raise ValueError("Something went wrong")
'''
ZIPCODEAPI

As nominatim sometimes is unable to provide a specific location with the city and instead returns
a postcode (zipcode) and the country name. For the suggestions that we built, the suggestions
require which city (city name) it is in order to look for other similar categoried services
in the area. Thus, this function takes in the INPUT of a zipcode, and RETURNS the name of the city.

'''
def zipcode_retrieval(zipcode):

    # Use this API key first.
    url = ZIP_HOST_URL + ZIPCODE_API_KEY + ZIP_FORMAT + zipcode + ZIP_DEGREE
    response = requests.request('GET', url=url)
    results = response.json()

    if "error_code" in results:
        # In case the first API key runs out of requests per hour.
        url = ZIP_HOST_URL + BACKUP_ZIP_KEY + ZIP_FORMAT + zipcode + ZIP_DEGREE
        response = requests.request('GET', url=url)
        return response.json()
    else:
        return results



def zipcode_to_city(zipcode):
    response = zipcode_retrieval(zipcode)
    return response['city']
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
    endpoint_categories = category_of_business_nominatim(end_lat, end_lon)
    business_locations = {}


    begin_string_address, begin_address_dict = return_address_from_location_nominatim(start_lat, start_lon)
    end_string_address, end_address_dict = return_address_from_location_nominatim(end_lat, end_lon)
    try:
        city = end_address_dict["city"]
    except:
        try:
            # To classify cities as towns, as some locations only appear as "TOWN" to nominatim
            city = end_address_dict["town"]
        except:
            try:
                # To classify cities through zipcode, as some locations only appear as "POSTCODE", so convert postcode to city
                zipcode = end_address_dict["postcode"]
                city = zipcode_to_city(zipcode)
            except:
                return {'message' : 'Sorry, the most recent trip was unable to be detected as to which city.', 'method': 'bike'}
    address = end_string_address
    start_lat_lon = start_lat + "," + start_lon
    end_lat_lon = end_lat + "," + end_lon
    location_review = review_start_loc_nominatim(end_lat, end_lon)
    ratings_bus = {}
    error_message = 'Sorry, unable to retrieve datapoint'
    error_message_categor = 'Sorry, unable to retrieve datapoint because datapoint is a house or datapoint does not belong in service categories'
    try:
        if (endpoint_categories):
            for categor in endpoint_categories:
                queried_bus = search(YELP_API_KEY, categor, city)['businesses']
                for q in queried_bus:
                    if q['rating'] >= location_review:
                        #'Coordinates' come out as two elements, latitude and longitude
                        ratings_bus[q['name']] = (q['rating'], q['alias'])
                        obtained = q['location']['display_address'][0] + q['location']['display_address'][1]
                        obtained.replace(' ', '+')
                        business_locations[q['name']] = obtained
        else:
            return {'message' : error_message_categor, 'question': None, 'suggested_loc': None, 'method': 'bike', 'rating': None, 'businessid': None}
    except:
        return {'message' : error_message_categor, 'question': None, 'suggested_loc': None, 'method': 'bike', 'rating': None, 'businessid': None}

    #THIS PART WILL BE FIXED ACCODRING TO NOMINATIM AND GET RID OF MAPQUEST (find some other way to calculate distance)
    for a in business_locations:
        try:
            calculate_distance = distance(start_lat_lon, business_locations[a])
        except:
            continue
        #Will check which mode the trip was taking for the integrated calculate yelp suggestion
        if calculate_distance < distance_in_miles and calculate_distance < 5 and calculate_distance >= 1:
            try:
                question = "How about this location?"
                new_message = "We saw that you took a vehicle from" + begin_string_address + "to" + address
                suggested_loc =  "Instead, there is " + a + "which has better reviews and closer to your original starting point"
                rating_mess = "Rating of " + str(ratings_bus[a][0])
                #Not sure to include the amount of carbon saved
                #Still looking to see what to return with this message, because currently my latitude and longitudes are stacked together in one string
                # insert_into_db(tripDict, i, yelp_suggestion_trips, uuid)
                return {'message' : new_message, 'question': question, 'suggested_loc': suggested_loc, 'method': 'bike', 'rating': str(ratings_bus[a][0]), 'businessid': ratings_bus[a][1]}
            except ValueError as e:
                continue
        elif calculate_distance < distance_in_miles and calculate_distance < 1:
            try:
                question = "How about this location?"
                new_message = "We saw that you took a vehicle from" + begin_string_address + "to" + address
                suggested_loc =  "Instead, there is " + a + "which has better reviews and closer to your original starting point"
                rating_mess = "Rating of " + str(ratings_bus[a][0])
                # insert_into_db(tripDict, i, yelp_suggestion_trips, uuid)
                return {'message' : new_message, 'question': question, 'suggested_loc': suggested_loc, 'method': 'walk', 'rating': str(ratings_bus[a][0]), 'businessid': ratings_bus[a][1]}
            except ValueError as e:
                continue
        elif calculate_distance < distance_in_miles and calculate_distance >= 5 and calculate_distance <= 15:
            try:
                question = "How about this location?"
                new_message = "We saw that you took a vehicle from" + begin_string_address + "to" + address
                suggested_loc =  "Instead, there is " + a + "which has better reviews and closer to your original starting point"
                rating_mess = "Rating of " + str(ratings_bus[a][0])
                # insert_into_db(tripDict, i, yelp_suggestion_trips, uuid)
                return {'message' : new_message, 'question': question, 'suggested_loc': suggested_loc, 'method': 'public', 'rating': str(ratings_bus[a][0]), 'businessid': ratings_bus[a][1]}
            except ValueError as e:
                continue

    return {'message': "Your endpoint has either been a non-serviceable category or a closeby option.",'method': 'public transportation', 'rating': None, 'businessid': None}

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
