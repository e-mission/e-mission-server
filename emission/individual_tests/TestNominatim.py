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
import emission.core.get_database as edb
import emission.core.common as cm
import unittest

from emission.core.wrapper.trip_old import Coordinate

import emission.net.ext_service.geocoder.nominatim as eco

class NominatimTest(unittest.TestCase):

    def test_make_url_geo(self):
        expected_result = "http://nominatim.openstreetmap.org/search?q=Golden%2C+Colorado&format=json"
        actual_result = eco.Geocoder.make_url_geo("Golden, Colorado")
        self.assertEqual(expected_result, actual_result)

    def test_get_json_geo(self):
        expected_result = [{'place_id': 230592559, 'licence': 'Data © OpenStreetMap contributors, ODbL 1.0. https://osm.org/copyright', 'osm_type': 'way', 'osm_id': 612835578, 'boundingbox': ['39.9041195', '39.914106', '-105.2365674', '-105.21353'], 'lat': '39.909856500000004', 'lon': '-105.22861864321705', 'display_name': 'NREL Flatiron Campus, Jefferson County, Colorado, United States', 'class': 'landuse', 'type': 'industrial', 'importance': 0.41000999999999993}, {'place_id': 165458796, 'licence': 'Data © OpenStreetMap contributors, ODbL 1.0. https://osm.org/copyright', 'osm_type': 'way', 'osm_id': 241338672, 'boundingbox': ['39.7385155', '39.7391893', '-105.1743745', '-105.1721491'], 'lat': '39.738987699999996', 'lon': '-105.17326231002255', 'display_name': 'NREL Employee Parking Garage, Denver West Parkway, Pleasant View, West Pleasant View, Jefferson County, Colorado, 80419, United States', 'class': 'amenity', 'type': 'parking', 'importance': 0.21000999999999997}, {'place_id': 165006264, 'licence': 'Data © OpenStreetMap contributors, ODbL 1.0. https://osm.org/copyright', 'osm_type': 'way', 'osm_id': 241338673, 'boundingbox': ['39.7385895', '39.7394212', '-105.1760441', '-105.1746968'], 'lat': '39.73896555', 'lon': '-105.17536976308806', 'display_name': 'NREL Parking Lot, Denver West Parkway, Pleasant View, West Pleasant View, Jefferson County, Colorado, 80419, United States', 'class': 'amenity', 'type': 'parking', 'importance': 0.21000999999999997}]
        actual_result = eco.Geocoder.get_json_geo("NREL, Colorado")
        self.assertEqual(expected_result, actual_result)

    def test_geocode(self):
        expected_result_lon = Coordinate(39.7546349, -105.220580).get_lon()
        expected_result_lat = Coordinate(39.7546349, -105.220580).get_lat()
        actual_result_lon = eco.Geocoder.geocode("Golden, Colorado").get_lon()
        actual_result_lat = eco.Geocoder.geocode("Golden, Colorado").get_lat()
        self.assertEqual(expected_result_lon, actual_result_lon)
        self.assertEqual(expected_result_lat, actual_result_lat)


    def test_make_url_reverse(self):
        expected_result = "http://nominatim.openstreetmap.org/reverse?lat=39.7406821&lon=-105.168522&format=json"
        actual_result = (eco.Geocoder.make_url_reverse(39.7406821, -105.1685220))
        self.assertEqual(expected_result, actual_result)
 
    def test_get_json_reverse(self):
        expected_result = {'place_id': 151856645, 'licence': 'Data © OpenStreetMap contributors, ODbL 1.0. https://osm.org/copyright', 'osm_type': 'way', 'osm_id': 193193531, 'lat': '39.74074185', 'lon': '-105.168658237606', 'display_name': 'Visitors Center, 15013, Denver West Parkway, Applewood, Jefferson County, Colorado, 80401, United States', 'address': {'building': 'Visitors Center', 'house_number': '15013', 'road': 'Denver West Parkway', 'village': 'Applewood', 'county': 'Jefferson County', 'state': 'Colorado', 'ISO3166-2-lvl4': 'US-CO', 'postcode': '80401', 'country': 'United States', 'country_code': 'us'}, 'boundingbox': ['39.7405892', '39.7409443', '-105.1687471', '-105.1683515']}
        actual_result = eco.Geocoder.get_json_reverse(39.7406821, -105.1685220)
        self.assertEqual(expected_result, actual_result)

    def test_reverse_geocode(self):
        expected_result = "Visitors Center, 15013, Denver West Parkway, Applewood, Jefferson County, Colorado, 80401, United States"
        actual_result = eco.Geocoder.reverse_geocode(39.7406821, -105.1685220)
        self.assertEqual(expected_result, actual_result)

if __name__ == '__main__':
    unittest.main()