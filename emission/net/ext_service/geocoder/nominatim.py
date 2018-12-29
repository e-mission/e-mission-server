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
def check_against_business_location(location='0, 0', address = ''):
    if not re.compile('^(\-?\d+(\.\d+)?),\s*(\-?\d+(\.\d+)?)$').match(location):
        raise ValueError('Location Invalid')
    base_url = NEARBY_URL
    location = 'location=' + location
    try:
        key_string = '&key=' + GOOGLE_MAPS_KEY
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

def return_address_from_location_google(location='0,0'):
    """
    Creates a Google Maps API call that returns the addresss given a lat, lon
    """
    if not re.compile('^(\-?\d+(\.\d+)?),\s*(\-?\d+(\.\d+)?)$').match(location):
        raise ValueError('Location Invalid')
    base_url = 'https://maps.googleapis.com/maps/api/geocode/json?'
    latlng = 'latlng=' + location
    try:
        #This try block is for our first 150,000 requests. If we exceed this, use Jack's Token.
        key_string = '&key=' + GOOGLE_MAPS_KEY
        url = base_url + latlng + key_string #Builds the url
        result = requests.get(url).json() #Gets google maps json file
        cleaned = result['results'][0]['address_components']


        #Address to check against value of check_against_business_location
        chk = cleaned[0]['long_name'] + ' ' + cleaned[1]['long_name'] + ', ' + cleaned[3]['long_name']
        business_tuple = check_against_business_location(location, chk)
        
        if business_tuple[0]: #If true, the lat, lon matches a business location and we return business name
            address_comp = cleaned[0]['long_name'] + ' ' + cleaned[1]['short_name']
            return business_tuple[1], cleaned[3]['short_name'], address_comp
        else: #otherwise, we just return the address
            return cleaned[0]['long_name'] + ' ' + cleaned[1]['short_name'] + ', ' + cleaned[3]['short_name']
    except:
        try:
            #Use Jack's Token in case of some invalid request problem with other API Token
            key_string = '&key=' + BACKUP_GOOGLE_MAPS_KEY
            url = base_url + latlng + key_string #Builds the url
            result = requests.get(url).json() #Gets google maps json file
            cleaned = result['results'][0]['address_components']
            #Address to check against value of check_against_business_location
            chk = cleaned[0]['long_name'] + ' ' + cleaned[1]['long_name'] + ', ' + cleaned[3]['long_name']
            business_tuple = check_against_business_location(location, chk)
            if business_tuple[0]: #If true, the lat, lon matches a business location and we return business name
                address_comp = cleaned[0]['long_name'] + ' ' + cleaned[1]['short_name'] 
                return business_tuple[1], cleaned[3]['short_name'], address_comp
            else: #otherwise, we just return the address
                return cleaned[0]['long_name'] + ' ' + cleaned[1]['short_name'] + ', ' + cleaned[3]['short_name']
        except:
            raise ValueError("Something went wrong")

def _do_google_geo(address):
    geo = pyGeo(GOOGLE_MAPS_KEY)
    results = geo.geocode(address)
    return Coordinate(results[0].coordinates[0], results[0].coordinates[1])

def _do_google_reverse(lat, lng):
    geo = pyGeo(GOOGLE_MAPS_KEY)
    address = geo.reverse_geocode(lat, lng)
    return address[0]
