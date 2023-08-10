import urllib.request, urllib.parse
import json
import unittest

class TestReverseGeocode(unittest.TestCase):
    def testCompareResult(self):
        #Didn't use the query in nominatim.json because it would change how we query nominatim regularly. 
        nominatim_reverse_query = "http://localhost:8080/reverse?"
        params = {
                "lat" : 41.831174, 
                "lon" : -71.414907,
                "format" : "json"
            }
        encoded_params = urllib.parse.urlencode(params)
        url = nominatim_reverse_query + encoded_params
        request = urllib.request.Request(url)
        response = urllib.request.urlopen(request)
        parsed_response = json.loads(response.read())
        # ndn = nominatim display name
        ndn = str(parsed_response.get("display_name"))
        #expected display name is a string 
        edn = "Rhode Island State Capitol Building, 82, Smith Street, Downtown, Providence, Providence County, 02903, United States"
        self.assertEqual(ndn, edn)
if __name__ == '__main__':
    unittest.main()