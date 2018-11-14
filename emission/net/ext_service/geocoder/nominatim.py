from __future__ import unicode_literals
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import
from future import standard_library
standard_library.install_aliases()
import os
from builtins import *
from builtins import object
import urllib.request, urllib.parse, urllib.error, urllib.request, urllib.error, urllib.parse
import logging
import json

from emission.core.wrapper.trip_old import Coordinate
from pygeocoder import Geocoder as pyGeo  ## We fall back on this if we have to

try:
    script_dir = os.path.dirname(__file__)
    rel_path = "../gmaps/googlemaps.json"
    abs_file_path = os.path.join(script_dir, rel_path)
    googlemaps_key_file = open(abs_file_path, 'r')
    GOOGLE_MAPS_KEY = json.load(googlemaps_key_file)["api_key"]
except Exception as e:
    print("google maps key not configured, falling back to nominatim")

try:
    script_dir = os.path.dirname(__file__)
    rel_path = "nominatim.json"
    abs_file_path = os.path.join(script_dir, rel_path)
    nominatim_file = open(abs_file_path, 'r')
    nominatim_config_object = json.load(nominatim_file)
    NOMINATIM_QUERY_URL = nominatim_config_object["query_url"]
    #nominatim_file.close()
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

        query_url = NOMINATIM_QUERY_URL + "/search.php?"
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
        try:
            jsn = cls.get_json_geo(address)
            lat = float(jsn[0]["lat"])
            lon = float(jsn[0]["lon"])
            return Coordinate(lat, lon)
        except Exception as e:
            print(e)
            print("defaulting")
            #TODO: Right now there is no default gecoder. Discuss if we should create a google account for this.
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
        # try:
        #     jsn = cls.get_json_reverse(lat, lng)
        #     address = jsn["display_name"]
        #     return address

        # except:
        #     print "defaulting"
        return _do_google_reverse(lat, lng) # Just in case

## Failsafe section
def _do_google_geo(address):
    geo = pyGeo(GOOGLE_MAPS_KEY)
    results = geo.geocode(address)
    return Coordinate(results[0].coordinates[0], results[0].coordinates[1])

def _do_google_reverse(lat, lng):
    geo = pyGeo(GOOGLE_MAPS_KEY)
    address = geo.reverse_geocode(lat, lng)
    return address[0]
