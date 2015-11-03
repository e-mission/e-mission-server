import urllib, urllib2
from emission.core.wrapper.trip_old import Coordinate
import json
from pygeocoder import Geocoder as pyGeo  ## We fall back on this if we have to

class Geocoder:

    def __init__(self):
        pass
        
    @classmethod
    def make_url_geo(cls, address):
        params = {
            "q" : address,
            "format" : "json"
        }

        query_url = "http://nominatim.openstreetmap.org/search?"
        encoded_params = urllib.urlencode(params)
        url = query_url + encoded_params
        return url

    @classmethod
    def get_json_geo(cls, address):
        request = urllib2.Request(cls.make_url_geo(address))
        response = urllib2.urlopen(request)
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

        query_url = "http://nominatim.openstreetmap.org/reverse?"
        encoded_params = urllib.urlencode(params)
        url = query_url + encoded_params
        return url

    @classmethod
    def get_json_reverse(cls, lat, lng):
        request = urllib2.Request(cls.make_url_reverse(lat, lng))
        response = urllib2.urlopen(request)
        print json.loads(response.read())
        return json.loads(response.read())

    @classmethod
    def reverse_geocode(cls, lat, lng):
        # try:
        #     jsn = cls.get_json_reverse(lat, lng)
        #     address = jsn["display_name"]
        # except:
        #     print "defaulting"
        return _do_google_reverse(lat, lng) # Just in case
        return address

## Failsafe section
def _do_google_geo(address):
    geo = pyGeo()
    results = geo.geocode(address)
    return Coordinate(results[0].coordinates[0], results[0].coordinates[1])

def _do_google_reverse(lat, lng):
    geo = pyGeo()
    address = geo.reverse_geocode(lat, lng)
    return address[0]
    