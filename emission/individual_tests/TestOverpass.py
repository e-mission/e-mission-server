from __future__ import unicode_literals
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import
from future import standard_library
standard_library.install_aliases()
from builtins import *
import unittest
import os
import requests
import emission.net.ext_service.transit_matching.match_stops as enetm
import logging

#Set up query
GEOFABRIK_OVERPASS_KEY = os.environ.get("GEOFABRIK_OVERPASS_KEY")

#Sample loc1 = NREL East Gate
loc1 = {'coordinates': [-105.16844103184974, 39.740428870224605]}
#Sample loc2 = Denver Union Station
loc2 = {'coordinates': [-105.00083982302972, 39.753710532185025]}
#Sample loc3 = Grand Junction Train Station, CO
loc3 = {'coordinates': [-108.57055213129632, 39.06472424640481]}

class OverpassTest(unittest.TestCase):
    def setUp(self):
        # Un-comment the two lines below to print debug logs.
        # loglevel = logging.DEBUG
        # logging.basicConfig(level=loglevel)
        sample_data = '[out:json][bbox];way[amenity=parking];out;&bbox=-122.1111238,37.4142118,-122.1055791,37.4187945'
        call_base = 'api/interpreter?data='
        self.public_url_base = 'https://lz4.overpass-api.de/'+ call_base + sample_data
        if GEOFABRIK_OVERPASS_KEY:
            self.gfbk_url_base = 'https://overpass.geofabrik.de/' + GEOFABRIK_OVERPASS_KEY + '/' + call_base + sample_data
        else:
            self.gfbk_url_base = None

    def test_overpass(self):
        r_gfbk = requests.get(self.gfbk_url_base)
        r_public = requests.get(self.public_url_base)
        
        if r_gfbk.status_code == 200 and r_public.status_code == 200:
            print("requests successful!")
            r_gfbk_len, r_public_len = len(r_gfbk.json()), len(r_public.json())
            self.assertEqual(r_gfbk_len, r_public_len)
        else:
            print("status_gfbk", r_gfbk.status_code, type(r_gfbk.status_code), "status_public", r_public.status_code)

    #Test utilizes the functions get_stops_near, get_public_transit_stops, and make_request_and_catch.  
    def test_get_stops_near(self):
        stops_at_loc1 = enetm.get_stops_near(loc1['coordinates'], 150.0)[0]
        first_stop_routes = stops_at_loc1[0]['routes']
        self.assertEqual(len(first_stop_routes), 2)
        first_stop_first_route_tags = first_stop_routes[0]['tags']
        expected_tags = {'from': 'National Renewable Energy Lab', 'name': 'RTD Route 125: Red Rocks College', 'network': 'RTD', 'network:wikidata': 'Q7309183', 'network:wikipedia': 'en:Regional Transportation District', 'operator': 'Regional Transportation District', 'public_transport:version': '1', 'ref': '125', 'route': 'bus', 'to': 'Red Rocks College', 'type': 'route'}
        self.assertEqual(first_stop_first_route_tags, expected_tags)
   
    #Get_stops_near generates two stops from the given coordinates.
    # Get_predicted_transit_mode finds a common route between them (train).
    def test_get_predicted_transit_mode(self):
        [stops_at_loc2, stops_at_loc3] = enetm.get_stops_near(
            [loc2['coordinates'], loc3['coordinates']],
            400.0,
        )
        actual_result = enetm.get_predicted_transit_mode(stops_at_loc2, stops_at_loc3)
        expected_result = ['train', 'train']
        self.assertEqual(actual_result, expected_result)

if __name__ == '__main__':
    unittest.main()

