from __future__ import unicode_literals
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import
from future import standard_library
standard_library.install_aliases()
from builtins import *
import unittest
import os
from emission.core.wrapper.trip_old import Coordinate
import emission.net.ext_service.geocoder.nominatim as eco

#temporarily sets NOMINATIM_QUERY_URL to the environment variable for testing.
eco.NOMINATIM_QUERY_URL = os.environ.get("NOMINATIM_QUERY_URL")
# print("query URL:", eco.NOMINATIM_QUERY_URL)

class NominatimTest(unittest.TestCase):

    def test_make_url_geo(self):
        expected_result = eco.NOMINATIM_QUERY_URL + "/search?q=Providence%2C+Rhode+Island&format=json"
        actual_result = eco.Geocoder.make_url_geo("Providence, Rhode Island")
        self.assertEqual(expected_result, actual_result)

    def test_get_json_geo(self):
        expected_result = [{'place_id': 133278818, 'licence': 'Data © OpenStreetMap contributors, ODbL 1.0. http://osm.org/copyright', 'osm_type': 'way', 'osm_id': 121496393, 'lat': '41.824034499999996', 'lon': '-71.41290469687814', 'class': 'amenity', 'type': 'townhall', 'place_rank': 30, 'importance': 0.2257940944999783, 'addresstype': 'amenity', 'name': 'Providence City Hall', 'display_name': 'Providence City Hall, Dorrance Street, Downtown, Providence, Providence County, Rhode Island, 02902, United States', 'boundingbox': ['41.8237547', '41.8243153', '-71.4132816', '-71.4125278']}]
        actual_result = eco.Geocoder.get_json_geo("Providence City Hall, Rhode Island")
        self.assertEqual(expected_result, actual_result)

    def test_geocode(self):
        expected_result_lon = Coordinate(41.8239891, -71.4128343).get_lon()
        expected_result_lat = Coordinate(41.8239891, -71.4128343).get_lat()
        actual_result_lon = eco.Geocoder.geocode("Providence, Rhode Island").get_lon()
        actual_result_lat = eco.Geocoder.geocode("Providence, Rhode Island").get_lat()
        self.assertEqual(expected_result_lon, actual_result_lon)
        self.assertEqual(expected_result_lat, actual_result_lat)


    def test_make_url_reverse(self):
        expected_result = eco.NOMINATIM_QUERY_URL + "/reverse?lat=41.8239891&lon=-71.4128343&format=json"
        actual_result = (eco.Geocoder.make_url_reverse(41.8239891, -71.4128343))
        self.assertEqual(expected_result, actual_result)
 
    def test_get_json_reverse(self):
        expected_result = {'place_id': 133278818, 'licence': 'Data © OpenStreetMap contributors, ODbL 1.0. http://osm.org/copyright', 'osm_type': 'way', 'osm_id': 121496393, 'lat': '41.824034499999996', 'lon': '-71.41290469687814', 'class': 'amenity', 'type': 'townhall', 'place_rank': 30, 'importance': 0.2257940944999783, 'addresstype': 'amenity', 'name': 'Providence City Hall', 'display_name': 'Providence City Hall, Dorrance Street, Downtown, Providence, Providence County, Rhode Island, 02902, United States', 'address': {'amenity': 'Providence City Hall', 'road': 'Dorrance Street', 'neighbourhood': 'Downtown', 'city': 'Providence', 'county': 'Providence County', 'state': 'Rhode Island', 'ISO3166-2-lvl4': 'US-RI', 'postcode': '02902', 'country': 'United States', 'country_code': 'us'}, 'boundingbox': ['41.8237547', '41.8243153', '-71.4132816', '-71.4125278']}
        actual_result = eco.Geocoder.get_json_reverse(41.8239891, -71.4128343)
        self.assertEqual(expected_result, actual_result)

    def test_reverse_geocode(self):
        expected_result = "Providence City Hall, Dorrance Street, Downtown, Providence, Providence County, Rhode Island, 02902, United States"
        actual_result = eco.Geocoder.reverse_geocode(41.8239891, -71.4128343)
        self.assertEqual(expected_result, actual_result)

if __name__ == '__main__':
    unittest.main()