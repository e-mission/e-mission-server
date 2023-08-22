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
NOMINATIM_QUERY_URL_env = os.environ.get("NOMINATIM_QUERY_URL", "")
NOMINATIM_QUERY_URL = NOMINATIM_QUERY_URL_env if NOMINATIM_QUERY_URL_env != "" else eco.NOMINATIM_QUERY_URL
print("query URL in TestNominatim:", NOMINATIM_QUERY_URL)

class NominatimTest(unittest.TestCase):
    maxDiff = None
    # def test_make_url_geo(self):
    #     expected_result = NOMINATIM_QUERY_URL + "/search?q=Providence%2C+Rhode+Island&format=json"
    #     actual_result = eco.Geocoder.make_url_geo("Providence, Rhode Island")
    #     self.assertEqual(expected_result, actual_result)

    def test_get_json_geo(self):
        expected_result = [{'place_id': 139763, 'licence': 'Data © OpenStreetMap contributors, ODbL 1.0. https://osm.org/copyright', 'osm_type': 'way', 'osm_id': 121496393, 'boundingbox': ['41.8237547', '41.8243153', '-71.4132816', '-71.4125278'], 'lat': '41.824034499999996', 'lon': '-71.41290469687814', 'display_name': 'Providence City Hall, Fulton Street, Downtown, Providence, Providence County, 02903, United States', 'class': 'amenity', 'type': 'townhall', 'importance': 1.25001}]
        actual_result = eco.Geocoder.get_json_geo("Providence City Hall, Fulton Street, Downtown, Providence, Providence County, 02903, United States")
        self.assertEqual(expected_result, actual_result)

    def test_geocode(self):
        expected_result_lon = Coordinate(41.8239891, -71.4128343).get_lon()
        expected_result_lat = Coordinate(41.8239891, -71.4128343).get_lat()
        actual_result = eco.Geocoder.geocode("Providence, Rhode Island")
        actual_result_lon = actual_result.get_lon()
        actual_result_lat = actual_result.get_lat()
        self.assertEqual(expected_result_lon, actual_result_lon)
        self.assertEqual(expected_result_lat, actual_result_lat)


    # def test_make_url_reverse(self):
    #     expected_result = NOMINATIM_QUERY_URL + "/reverse?lat=41.8239891&lon=-71.4128343&format=json"
    #     actual_result = (eco.Geocoder.make_url_reverse(41.8239891, -71.4128343))
    #     self.assertEqual(expected_result, actual_result)
 
 #started modifying this test to potentially use three results: ground truth, nominatim in docker container (specific version), and regular nominatim query (most current version)
 #if this is necessary, it will help us see if the container or query needs to be updated for nominatim compatibility.
    def test_get_json_reverse(self):
        expected_result = {'place_id': 139763, 'licence': 'Data © OpenStreetMap contributors, ODbL 1.0. https://osm.org/copyright', 'osm_type': 'way', 'osm_id': 121496393, 'lat': '41.824034499999996', 'lon': '-71.41290469687814', 'display_name': 'Providence City Hall, Fulton Street, Downtown, Providence, Providence County, 02903, United States', 'address': {'amenity': 'Providence City Hall', 'road': 'Fulton Street', 'neighbourhood': 'Downtown', 'city': 'Providence', 'county': 'Providence County', 'postcode': '02903', 'country': 'United States', 'country_code': 'us'}, 'boundingbox': ['41.8237547', '41.8243153', '-71.4132816', '-71.4125278']}
        actual_docker = eco.Geocoder.get_json_reverse(41.8239891, -71.4128343)
        # actual_nominatim = "httsp://nominatim.openstreetmap.org/"
        self.assertEqual(expected_result, actual_docker)
        # self.assertEqual(actual_docker, actual_nominatim)

    def test_reverse_geocode(self):
        expected_result = "Portugal Parkway, Fox Point, Providence, Providence County, 02906, United States"
        actual_result = eco.Geocoder.reverse_geocode(41.8174476, -71.3903767)
        self.assertEqual(expected_result, actual_result)
    
#this test was written with the intention of using a ground truth file. Once a fake trip is generated in Rhode island, this section will be modified.  
    # def test_display_name(self):
    #     nominatim_reverse_query = NOMINATIM_QUERY_URL + "/reverse?"
    #     params = {
    #             "lat" : 41.831174, 
    #             "lon" : -71.414907,
    #             "format" : "json"
    #         }
    #     encoded_params = urllib.parse.urlencode(params)
    #     url = nominatim_reverse_query + encoded_params
    #     request = urllib.request.Request(url)
    #     response = urllib.request.urlopen(request)
    #     parsed_response = json.loads(response.read())
    #     actual_result = str(parsed_response.get("display_name"))
    #     expected_result = "Rhode Island State Capitol Building, 82, Smith Street, Downtown, Providence, Providence County, 02903, United States"
    #     self.assertEqual(expected_result, actual_result)
if __name__ == '__main__':
    unittest.main()