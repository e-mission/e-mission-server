import os
import unittest

import requests


class OverpassAPITest(unittest.TestCase):
    def test_overpass(self):
        key = os.environ.get("GEOFABRIK_OVERPASS_KEY")
        self.assertTrue(key, "GEOFABRIK_OVERPASS_KEY is required")
        endpoints = {
            "geofabrik": f"https://overpass.geofabrik.de/{key}/api/interpreter",
            "public": "https://lz4.overpass-api.de/api/interpreter",
        }
        queries = {
            "parking": (
                '[out:json];way["amenity"="parking"]'
                '(37.4142118,-122.1111238,37.4187945,-122.1055791);out body;',
                "way",
                {"amenity": "parking"},
            ),
            "route 125": (
                '[out:json];relation["route"="bus"]["ref"="125"]'
                '(39.73908,-105.17020,39.74178,-105.16668);out body;',
                "relation",
                {"ref": "125", "route": "bus", "network": "RTD",
                 "from": "National Renewable Energy Lab", "to": "Red Rocks College"},
            ),
        }
        for endpoint_name, url in endpoints.items():
            for query_name, (query, expected_type, expected_tags) in queries.items():
                with self.subTest(endpoint=endpoint_name, query=query_name):
                    try:
                        response = requests.get(url, params={"data": query}, timeout=60)
                    except requests.RequestException:
                        response = None
                    self.assertIsNotNone(response, f"{endpoint_name} request failed")
                    self.assertEqual(200, response.status_code)
                    elements = response.json()["elements"]
                    self.assertTrue(any(
                        element.get("type") == expected_type and
                        all(element.get("tags", {}).get(k) == v
                            for k, v in expected_tags.items())
                        for element in elements
                    ), f"{endpoint_name} returned no expected {query_name} element")


if __name__ == '__main__':
    unittest.main()
