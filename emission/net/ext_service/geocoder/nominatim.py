from __future__ import unicode_literals
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import
from future import standard_library
standard_library.install_aliases()
from builtins import *
from builtins import object
import urllib.request, urllib.parse, urllib.error, urllib.request, urllib.error, urllib.parse
import logging
import json
import re
import requests

from emission.core.wrapper.trip_old import Coordinate
from pygeocoder import Geocoder as pyGeo  ## We fall back on this if we have to
from math import sin, cos, sqrt, atan2, radians

try:
    googlemaps_key_file = open("conf/net/ext_service/googlemaps.json")
    googlemaps_json = json.load(googlemaps_key_file)
    GOOGLE_MAPS_KEY = googlemaps_json["access_token"]
    BACKUP_GOOGLE_MAPS_KEY = googlemaps_json["backup_access_token"]
    NEARBY_URL = googlemaps_json["nearby_base_url"]
except:
    print("google maps key not configured, falling back to nominatim")

try:
    nominatim_file = open("conf/net/ext_service/nominatim.json")
    NOMINATIM_QUERY_URL = json.load(nominatim_file)["query_url"]
except:
    print("nominatim not configured either, place decoding must happen on the client")

class Geocoder(object):
 
    def __init__(self):
        pass
        
    @classmethod
    def make_url_geo(cls, address):
        params = {
            "q" : address,
            "format" : "json"
        }

        query_url = NOMINATIM_QUERY_URL + "/search?"
        encoded_params = urllib.parse.urlencode(params)
        url = query_url + encoded_params
        logging.debug("For geocoding, using URL %s" % url)
        return url

    @classmethod
    def get_json_geo(cls, address):
        request = urllib.request.Request(cls.make_url_geo(address))
        response = urllib.request.urlopen(request)
        jsn = json.loads(response.read())
        return jsn

    @classmethod
    def geocode(cls, address):
        # try:
        #     jsn = cls.get_json_geo(address)
        #     lat = float(jsn[0]["lat"])
        #     lon = float(jsn[0]["lon"])
        #     return Coordinate(lat, lon)
        # except:
        #     print "defaulting"
        return _do_google_geo(address) # If we fail ask the gods


    @classmethod
    def make_url_reverse(cls, lat, lon):
        params = {
            "lat" : lat, 
            "lon" : lon,
            "format" : "json"
        }

        query_url = NOMINATIM_QUERY_URL + "/reverse?"
        encoded_params = urllib.parse.urlencode(params)
        url = query_url + encoded_params
        logging.debug("For reverse geocoding, using URL %s" % url)
        return url

    @classmethod
    def get_json_reverse(cls, lat, lng):
        request = urllib.request.Request(cls.make_url_reverse(lat, lng))
        response = urllib.request.urlopen(request)
        parsed_response = json.loads(response.read())
        logging.debug("parsed_response = %s" % parsed_response)
        return parsed_response

    @classmethod
    def reverse_geocode(cls, lat, lng):
        try:
            jsn = cls.get_json_reverse(lat, lng)
            business_name = jsn["display_name"]
            address = jsn["address"]
            return business_name, address
        except:
            print("defaulting")
            return _do_google_reverse(lat, lng) # Just in case

## Failsafe section

'''
GOOGLE LOOKUP VERS: Function that RETURNS a list of business locations near or at the latitude 
and longitude point given. 

Uses the helper function check_against_business_location. 

The lat, lon are converted into a string, so that it is easier to have it all in one variable 
in querying for the results through the API call. 

Moved these functions from suggestion_sys.py to nominatim.py because wanted to reduce the 
number of times of opening the same json file and nominatim.py was already calling 
the google maps and nominatim json files, so that's why these lookup functions are now 
in nominatim.py

Attempted to replace the google reverse lookup function with the nominatim.py version of the 
function, but it encountered an error in the pygeocoder file that is in anaconda.
Thus, decided to move the original google reverse functions from suggestion_sys.py 
to nominatim.py
'''
def check_against_business_location(lat, lon, address = ''):
    location_first = lat + ',' + lon
    if not re.compile('^(\-?\d+(\.\d+)?),\s*(\-?\d+(\.\d+)?)$').match(location_first):
        raise ValueError('Location Invalid')
    base_url = NEARBY_URL
    location = 'location=' + location_first
    try:
        key_string = '&key=' + GOOGLE_MAPS_KEY
        radius = '&radius=10'
        url = base_url + location + radius + key_string
        result = requests.get(url).json()
        cleaned = result['results']
        for i in cleaned:
            logging.debug("For amenity %s, comparing address %s with nearby business %s" %
                (i['name'], address, i['vicinity']))
            #If the street address matches the street address of this business, we return a tuple
            #signifying success and the business name
            if address == i['vicinity']:
                return (True, i['name'])
        else:
            return (False, '')
    except:
        try:
            key_string = '&key=' + BACKUP_GOOGLE_MAPS_KEY
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

def return_address_from_location_google(lat, lon):
    """
    Creates a Google Maps API call that returns the addresss given a lat, lon
    """
    location = lat + ',' + lon
    if not re.compile('^(\-?\d+(\.\d+)?),\s*(\-?\d+(\.\d+)?)$').match(location):
        raise ValueError('Location Invalid')
    base_url = 'https://maps.googleapis.com/maps/api/geocode/json?'
    latlng = 'latlng=' + location
    try:
        #This try block is for our first 150,000 requests. If we exceed this, use Jack's Token.
        key_string = '&key=' + GOOGLE_MAPS_KEY
        url = base_url + latlng + key_string #Builds the url
        # logging.debug("About to query google with URL %s" % url)
        result = requests.get(url).json() #Gets google maps json file
        cleaned = result['results'][0]['address_components']
        logging.debug("Components from address lookup = %s" % cleaned)
        #Address to check against value of check_against_business_location
        chk = cleaned[0]['long_name'] + ' ' + cleaned[1]['long_name'] + ', ' + cleaned[3]['long_name']
        business_tuple = check_against_business_location(lat, lon, chk)
        logging.debug("After checking = %s, got business tuple %s " % (chk, business_tuple))
        location_is_service = isLocationService(cleaned)
        address_comp = cleaned[0]['long_name'] + ' ' + cleaned[1]['short_name']
        if business_tuple[0]: #If true, the lat, lon matches a business location and we return business name
            #, cleaned[3]['short_name'], address_comp
            business_name = business_tuple[1]
        else:
            business_name = None

        return business_name, address_comp, cleaned[3]['short_name'], location_is_service
    except:
        try:
            #Use Jack's Token in case of some invalid request problem with other API Token
            key_string = '&key=' + BACKUP_GOOGLE_MAPS_KEY
            url = base_url + latlng + key_string #Builds the url
            result = requests.get(url).json() #Gets google maps json file
            cleaned = result['results'][0]['address_components']
            location_is_service = isLocationService(cleaned)
            #Address to check against value of check_against_business_location
            chk = cleaned[0]['long_name'] + ' ' + cleaned[1]['long_name'] + ', ' + cleaned[3]['long_name']
            business_tuple = check_against_business_location(lat, lon, chk)
            if business_tuple[0]: #If true, the lat, lon matches a business location and we return business name
                address_comp = cleaned[0]['long_name'] + ' ' + cleaned[1]['short_name'] 
                return business_tuple[1], cleaned[3]['short_name'], address_comp, location_is_service
            else: #otherwise, we just return the address
                return cleaned[0]['long_name'] + ' ' + cleaned[1]['short_name'] + ', ' + cleaned[3]['short_name'], location_is_service
        except:
            raise ValueError("Something went wrong")

'''
Function that checks if location was a place of service or a residential area. RETURNS TRUE 
if it is a location of service, FALSE otherwise
'''
def isLocationService(address_components):
    for a in address_components:
        types_of_service = a["types"]
        longname = a["long_name"]
        for t in types_of_service:
            if t == "premise" or t == "neighborhood" and "Downtown" not in longname:
                return False
    return True

def _do_google_geo(address):
    geo = pyGeo(GOOGLE_MAPS_KEY)
    results = geo.geocode(address)
    return Coordinate(results[0].coordinates[0], results[0].coordinates[1])

def _do_google_reverse(lat, lng):
    geo = pyGeo(GOOGLE_MAPS_KEY)
    address = geo.reverse_geocode(lat, lng)
    return address[0]

#google API call to find nearby places
def places_nearby_google(lat, lon):
    trial_type = "restaurant"
    url = "https://maps.googleapis.com/maps/api/place/nearbysearch/json?location=" + lat + "," + lon + "&radius=6" + "&type=" + trial_type + "&key=" + GOOGLE_MAPS_KEY
    result = requests.get(url).json()
    i = 0
    types = ["art_gallery", "bank", "bakery", "beauty_salon", "cafe", "city_hall", "clothing_store", "convenience_store", "department_store", "dentist", "gas_station", "hospital", "jewelry_store", "local_government_office", "lodging", "library", "school", "spa", "store", "supermarket"]
    while len(result.get("results")) == 0 and i < len(types) - 1:
        i += 1
        trial_type = types[i]
        print(trial_type)
        url = "https://maps.googleapis.com/maps/api/place/nearbysearch/json?location=" + lat + "," + lon + "&radius=6" + "&type=" + trial_type + "&key=" + GOOGLE_MAPS_KEY
        result = requests.get(url).json()
    if i == len(types) - 1:
        return "No result found for this location"
    return result.get("results")[0].get("name")

def places_nearby_google_using_address(lat, lon):
    address = return_address_from_location_google(lat, lon)
    street_and_number = address[1]
    street_and_number = street_and_number.split(" ")
    city = address[2]
    city = city.split(" ")
    url = "https://maps.googleapis.com/maps/api/place/findplacefromtext/json?input="
    for i in range(0, len(street_and_number)):
        url += "%20" + street_and_number[i]
    url += "%2C"
    for i in range(len(city)):
        url += "%20" + city[i]
    url += "&inputtype=textquery&key=" + GOOGLE_MAPS_KEY + "&fields=formatted_address,name,place_id,opening_hours,rating,types" + "&locationbias=circle:100@" + lat + "," + lon
    return requests.get(url).json()

def businesses_nearby_google(lat, lon):
    address = return_address_from_location_google(lat, lon)
    start_url = "https://maps.googleapis.com/maps/api/place/textsearch/json?query=businesses+near"
    street_and_number = address[1]
    street_and_number = street_and_number.split(" ")
    city = address[2]
    city = city.split(" ")
    for i in range(0, len(street_and_number)):
        start_url += "+" + street_and_number[i]
    for i in range(0, len(city)):
        start_url += "+" + city[i]
    start_url += "&radius=1" + "&key=" + GOOGLE_MAPS_KEY
    businesses = {}
    types = ["restaurant", "art_gallery", "bank", "bakery", "beauty_salon", "cafe", "city_hall", "clothing_store", "convenience_store", "department_store", "dentist", "gas_station", "hospital", "jewelry_store", "local_government_office", "lodging", "library", "school", "shopping_center", "spa", "store", "supermarket"]
    i = 0
    while i < len(types) - 1:
        url = start_url + "&type=" + types[i]
        result = requests.get(url).json()
        for j in range(len(result.get("results"))):
            businesses[result.get("results")[j].get("name")] = distance_latitude_longitude(float(lat), float(lon), 
                float(result.get("results")[j].get("geometry").get("location").get("lat")),
                float(result.get("results")[j].get("geometry").get("location").get("lng")))
        i += 1
    print(sorted(businesses, key=businesses.get, reverse = False))
    return sorted(businesses, key=businesses.get, reverse = False)[0]

def distance_latitude_longitude(lat1, lon1, lat2, lon2):
    R = 6373.0
    lat1 = radians(lat1)
    lon1 = radians(lon1)
    lat2 = radians(lat2)
    lon2 = radians(lon2)
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat / 2)**2 + cos(lat1) * cos(lat2) * sin(dlon / 2)**2
    c = 2 * atan2(sqrt(a), sqrt(1 - a))
    distance = R * c
    return distance 

