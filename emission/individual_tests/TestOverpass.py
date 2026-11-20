from builtins import *
import unittest
import os
import uuid
import json
import requests
import emission.net.ext_service.transit_matching.match_stops as enetm
import logging

#Set up query
GEOFABRIK_OVERPASS_KEY = os.environ.get("GEOFABRIK_OVERPASS_KEY")

#Sample loc1 = NLR East Gate
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
        self.gfbk_url_base = 'https://overpass.geofabrik.de/' + GEOFABRIK_OVERPASS_KEY + '/' + call_base + sample_data

    def test_overpass(self):
        r_gfbk = requests.get(self.gfbk_url_base)
        r_public = requests.get(self.public_url_base)
        
        if r_gfbk.status_code == 200 and r_public.status_code == 200:
            print("requests successful!")
            r_gfbk_len, r_public_len = len(r_gfbk.json()), len(r_public.json())
            self.assertEqual(r_gfbk_len, r_public_len)
        else:
            print("status_gfbk", r_gfbk.status_code, type(r_gfbk.status_code), "status_public", r_public.status_code)

    # This exercises the live Overpass request and route matching path.
    def test_get_stops_near(self):
        stops = enetm.get_stops_near(loc1, 150.0)
        self.assertTrue(stops, "Overpass returned no stops near NLR East Gate")
        route_125 = next(
            (route for stop in stops for route in stop["routes"]
             if route["tags"].get("ref") == "125"),
            None)
        self.assertIsNotNone(route_125, "Overpass results did not include RTD route 125")
        actual_result = route_125["tags"]
        print(f"Received result {actual_result}")
        # This will probably fail once the name changes in OSM, but RTD hasn't even changed the name of the stop yet so it may be a while
        # I say let's just let that happen - it's proof that the tests are working!
        # tests failed! OSM was finally updated! Fixing...
        expected_result = {'description': 'RTD Route 125: Red Rocks College','from': 'National Renewable Energy Lab', 'name': 'Red Rocks College', 'network': 'RTD', 'network:wikidata': 'Q7309183', 'network:wikipedia': 'en:Regional Transportation District', 'operator:short': 'RTD', 'operator': 'Regional Transportation District', 'public_transport:version': '1', 'ref': '125', 'route': 'bus', 'to': 'Red Rocks College', 'type': 'route'}
        self.assertEqual(expected_result, actual_result)
   
    # Get_stops_near finds stops at both ends; matching their live data predicts train.
    def test_get_predicted_transit_mode(self):
        stop1 = enetm.get_stops_near(loc2, 400.0)
        stop2 = enetm.get_stops_near(loc3, 400.0)
        self.assertTrue(stop1, "Overpass returned no stops near Denver Union Station")
        self.assertTrue(stop2, "Overpass returned no stops near Grand Junction Station")
        actual_result = enetm.get_predicted_transit_mode(stop1, stop2)
        expected_result = ['train', 'train']
        self.assertEqual(actual_result, expected_result)

    def test_intake_pipeline_with_live_overpass(self):
        import emission.core.get_database as edb
        import emission.pipeline.intake_stage as epi
        import emission.storage.decorations.analysis_timeseries_queries as esda
        import emission.tests.common as etc

        data_file = "emission/tests/data/real_examples/jack_untracked_time_2023-03-12"
        with open(data_file + ".inferred_section.expected_composite_trips") as expected_file:
            expected_trips = json.load(expected_file)
        expected_bus_intervals = [
            (section["data"]["start_ts"], section["data"]["end_ts"])
            for trip in expected_trips
            for section in trip["data"].get("sections", [])
            if section["data"].get("sensed_mode_str") == "BUS"
        ]
        self.assertTrue(expected_bus_intervals, "Ground truth contains no BUS sections")

        self.testEmail = f"overpass-pipeline-{uuid.uuid4()}@example.com"
        try:
            etc.setupRealExample(self, data_file)
            epi.run_intake_pipeline_for_user(self.testUUID)

            predictions = esda.get_entries(
                "inference/prediction", self.testUUID, time_query=None)
            predicted_bus_intervals = {
                (prediction.data.start_ts, prediction.data.end_ts)
                for prediction in predictions
                if "BUS" in prediction.data.predicted_mode_map
            }
            for interval in expected_bus_intervals:
                self.assertIn(
                    interval, predicted_bus_intervals,
                    f"Expected live pipeline to infer BUS for interval {interval}")
        finally:
            if hasattr(self, "testUUID"):
                for collection in (
                    edb.get_timeseries_db(),
                    edb.get_analysis_timeseries_db(),
                    edb.get_profile_db(),
                    edb.get_usercache_db(),
                    edb.get_pipeline_state_db(),
                ):
                    collection.delete_many({"user_id": self.testUUID})
                edb.get_uuid_db().delete_one({"user_email": self.testEmail})

if __name__ == '__main__':
    unittest.main()

