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
import attrdict as ad
import math

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
        stops_at_loc1 = enetm.get_stops_near([loc1['coordinates']], 150.0)[0]
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

    def test_overpass_request_count(self):
        """
        Test that get_stops_near makes the expected number of Overpass API calls.
        With MAX_BBOXES_PER_QUERY set to 10, we expect:
        - 10 coordinates -> 1 API call
        - 11 coordinates -> 2 API calls
        - 20 coordinates -> 2 API calls
        - 21 coordinates -> 3 API calls
        """
        original_max = enetm.MAX_BBOXES_PER_QUERY
        enetm.MAX_BBOXES_PER_QUERY = 10

        # Save the original make_request_and_catch
        original_make_request_and_catch = enetm.make_request_and_catch

        # Set up a counter for API calls.
        def dummy_make_request_and_catch(query):
            dummy_make_request_and_catch.call_count += 1
            # Return a dummy response that represents one chunk:
            return [{'id': 100, 'type': 'node', 'tags': {'dummy': True}},
                    {'type': 'count'}]
        dummy_make_request_and_catch.call_count = 0

        enetm.make_request_and_catch = dummy_make_request_and_catch

        # Define test cases: number of coordinates -> expected API calls.
        test_cases = {
            10: 1,  # 10 locations should be a single batch.
            11: 2,  # 11 locations: 11/10 => ceil(1.1) = 2 batches.
            20: 2,  # 20 locations: exactly 2 batches.
            21: 3,  # 21 locations: ceil(2.1) = 3 batches.
        }

        for num_coords, expected_calls in test_cases.items():
            # Reset the counter for each sub-test.
            dummy_make_request_and_catch.call_count = 0
            # Create dummy coordinates.
            coords = [[i, i + 0.5] for i in range(num_coords)]
            # Call get_stops_near, which will use our dummy_make_request_and_catch.
            _ = enetm.get_stops_near(coords, 150.0)
            self.assertEqual(dummy_make_request_and_catch.call_count, expected_calls,
                            msg=f"For {num_coords} coordinates, expected {expected_calls} API calls, got {dummy_make_request_and_catch.call_count}.")

        # Restore original settings.
        enetm.MAX_BBOXES_PER_QUERY = original_max
        enetm.make_request_and_catch = original_make_request_and_catch
        
if __name__ == '__main__':
    unittest.main()

