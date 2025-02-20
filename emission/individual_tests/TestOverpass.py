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

    def test_chunk_list(self):
        # Case 1: List of 10 elements with chunk size of 3.
        data = list(range(1, 11))  # [1, 2, ..., 10]
        chunk_size = 3
        chunks = list(enetm.chunk_list(data, chunk_size))
        expected_chunks = [[1, 2, 3], [4, 5, 6], [7, 8, 9], [10]]
        self.assertEqual(chunks, expected_chunks)

        # Case 2: Exact division
        data_exact = list(range(1, 10))  # [1, 2, ..., 9]
        chunk_size = 3
        chunks_exact = list(enetm.chunk_list(data_exact, chunk_size))
        expected_chunks_exact = [[1, 2, 3], [4, 5, 6], [7, 8, 9]]
        self.assertEqual(chunks_exact, expected_chunks_exact)

        # Case 3: Empty list
        data_empty = []
        chunks_empty = list(enetm.chunk_list(data_empty, chunk_size))
        self.assertEqual(chunks_empty, [])

    def test_get_stops_near_many_chunks(self):
        """
        Test get_stops_near with many chunks.
        Override MAX_BBOXES_PER_QUERY to 1 so that each coordinate produces its own chunk.
        Supply 20 dummy coordinates and verify that 20 chunks are returned.
        """
        original_max = enetm.MAX_BBOXES_PER_QUERY
        enetm.MAX_BBOXES_PER_QUERY = 1

        # Create 20 dummy coordinates ([lon, lat]).
        coords = [[i, i + 0.5] for i in range(20)]

        # Patch make_request_and_catch to return a dummy response.
        original_make_request_and_catch = enetm.make_request_and_catch
        def dummy_make_request_and_catch(query):
            # Return a dummy response: one dummy node and a "count" marker.
            return [{'id': 100, 'type': 'node', 'tags': {'dummy': True}},
                    {'type': 'count'}]
        enetm.make_request_and_catch = dummy_make_request_and_catch

        stops = enetm.get_stops_near(coords, 150.0)
        # Expect one chunk per coordinate = 20 chunks.
        self.assertEqual(len(stops), 20)
        for chunk in stops:
            # Each chunk (from the dummy response) should contain one stop.
            self.assertEqual(len(chunk), 1)
            self.assertEqual(chunk[0]['tags'], {'dummy': True})

        # Restore original settings.
        enetm.MAX_BBOXES_PER_QUERY = original_max
        enetm.make_request_and_catch = original_make_request_and_catch

    def test_get_predicted_transit_mode_many_chunks(self):
        """
        Test get_predicted_transit_mode when provided with many stops.
        Simulate two sets (start and end) of 20 stops each, where each stop carries
        a unique route (with matching ids across both sets). Expect one matching route per stop.
        """
        start_stops = []
        end_stops = []
        for i in range(20):
            # Create a dummy route with a unique id and route "train".
            # Include a "ref" key to avoid an AttributeError.
            route = ad.AttrDict({
                'id': i,
                'tags': {'route': 'train', 'ref': str(i)}
            })
            stop_start = ad.AttrDict({
                'id': i,
                'tags': {},
                'routes': [route]
            })
            stop_end = ad.AttrDict({
                'id': i,
                'tags': {},
                'routes': [route]
            })
            start_stops.append(stop_start)
            end_stops.append(stop_end)
        
        actual_result = enetm.get_predicted_transit_mode(start_stops, end_stops)
        expected_result = ['train'] * 20
        self.assertEqual(actual_result, expected_result)

    def test_get_stops_near_different_batch_sizes(self):
        """
        Test get_stops_near using varying batch sizes.
        For each batch size, override MAX_BBOXES_PER_QUERY and supply a fixed list of dummy
        coordinates. Verify that the number of returned chunks equals ceil(total_coords / batch_size).
        """
        original_max = enetm.MAX_BBOXES_PER_QUERY
        original_make_request_and_catch = enetm.make_request_and_catch

        # Create 7 dummy coordinates.
        coords = [[i, i + 0.5] for i in range(7)]

        # Dummy response: one dummy node and a "count" marker.
        def dummy_make_request_and_catch(query):
            return [{'id': 100, 'type': 'node', 'tags': {'dummy': True}},
                    {'type': 'count'}]
        enetm.make_request_and_catch = dummy_make_request_and_catch

        for batch_size in [1, 2, 5, 10]:
            enetm.MAX_BBOXES_PER_QUERY = batch_size
            stops = enetm.get_stops_near(coords, 150.0)
            expected_chunks = math.ceil(len(coords) / batch_size)
            self.assertEqual(len(stops), expected_chunks,
                             msg=f"Batch size {batch_size} produced {len(stops)} chunks; expected {expected_chunks}.")
            for chunk in stops:
                self.assertEqual(len(chunk), 1)
                self.assertEqual(chunk[0]['tags'], {'dummy': True})

        # Restore original settings.
        enetm.MAX_BBOXES_PER_QUERY = original_max
        enetm.make_request_and_catch = original_make_request_and_catch

    def test_get_predicted_transit_mode_different_sizes(self):
        """
        Test get_predicted_transit_mode for different numbers of stops.
        For various sizes, simulate matching start and end stops and verify the expected matching routes.
        """
        for size in [1, 3, 7, 20]:
            start_stops = []
            end_stops = []
            for i in range(size):
                route = ad.AttrDict({
                    'id': i,
                    'tags': {'route': 'train', 'ref': str(i)}
                })
                stop_start = ad.AttrDict({
                    'id': i,
                    'tags': {},
                    'routes': [route]
                })
                stop_end = ad.AttrDict({
                    'id': i,
                    'tags': {},
                    'routes': [route]
                })
                start_stops.append(stop_start)
                end_stops.append(stop_end)
            actual_result = enetm.get_predicted_transit_mode(start_stops, end_stops)
            expected_result = ['train'] * size
            self.assertEqual(actual_result, expected_result,
                             msg=f"For {size} stops, expected {expected_result} but got {actual_result}.")


if __name__ == '__main__':
    unittest.main()

