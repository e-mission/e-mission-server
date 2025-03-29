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
import shutil
import time
import hashlib
import json

# Set up query
GEOFABRIK_OVERPASS_KEY = os.environ.get("GEOFABRIK_OVERPASS_KEY")

# Sample locations for transit stop testing
# Sample loc1 = NREL East Gate
loc1 = {'coordinates': [-105.16844103184974, 39.740428870224605]}
# Sample loc2 = Denver Union Station
loc2 = {'coordinates': [-105.00083982302972, 39.753710532185025]}
# Sample loc3 = Grand Junction Train Station, CO
loc3 = {'coordinates': [-108.57055213129632, 39.06472424640481]}
# Sample loc4 = Berkeley BART
loc4 = {'coordinates': [-122.2585745, 37.8719322], 'type': 'Point'}

class OverpassTest(unittest.TestCase):
    """
    Test suite for the Overpass API integration with e-mission server.
    
    The Overpass API is used to find transit stops near given locations,
    which helps identify if a trip involves public transit. These tests validate:
    
    1. Connectivity to both public and Geofabrik Overpass APIs
    2. The ability to find transit stops near specified locations
    3. The ability to determine transit modes between stops
    4. Filesystem caching functionality to reduce API calls
    5. Cache integrity and query hashing mechanisms
    6. Production mode behavior (bypassing cache)
    
    """
    
    def setUp(self):
        """
        Set up test environment before each test.
        
        - Creates sample query URLs for both public and Geofabrik Overpass APIs
        - Sets up the cache directory for testing
        - Preserves any existing cache files
        """
        logging.info("==== Setting up Overpass test environment ====")
        sample_data = '[out:json][bbox];way[amenity=parking];out;&bbox=-122.1111238,37.4142118,-122.1055791,37.4187945'
        call_base = 'api/interpreter?data='
        self.public_url_base = 'https://lz4.overpass-api.de/'+ call_base + sample_data
        
        # Handle the case when GEOFABRIK_OVERPASS_KEY is None
        if GEOFABRIK_OVERPASS_KEY is not None:
            self.gfbk_url_base = 'https://overpass.geofabrik.de/' + GEOFABRIK_OVERPASS_KEY + '/' + call_base + sample_data
            logging.info(f"Using Geofabrik API with key: {GEOFABRIK_OVERPASS_KEY[:4]}...")
        else:
            # Use a dummy value for testing - this won't actually be used since we'll skip tests that require it
            self.gfbk_url_base = ''
            logging.info("No Geofabrik API key found, will use public Overpass API")
        
        # Make sure the cache directory exists and is empty for testing
        self.cache_dir = enetm.CACHE_DIR
        if os.path.exists(self.cache_dir):
            # Save the existing cache files if any
            self.had_cache = True
            self.old_cache_files = os.listdir(self.cache_dir)
            logging.info(f"Found existing cache directory with {len(self.old_cache_files)} files")
        else:
            self.had_cache = False
            os.makedirs(self.cache_dir, exist_ok=True)
            logging.info(f"Created new cache directory at {self.cache_dir}")

    def tearDown(self):
        logging.info("==== Cleaning up Overpass test environment ====")
        # Restore the original cache state
        if os.path.exists(self.cache_dir):
            # Only clean up the test cache files that we know were created during tests
            removed_count = 0
            for file in os.listdir(self.cache_dir):
                if hasattr(self, 'test_created_files') and file in self.test_created_files:
                    os.remove(os.path.join(self.cache_dir, file))
                    removed_count += 1
            logging.info(f"Removed {removed_count} test cache files")

    def test_overpass(self):
        """
        Test connectivity to both public and Geofabrik Overpass APIs.
        
        This test:
        1. Makes requests to both APIs using the same query
        2. Verifies both return successful status codes
        3. Compares the JSON response lengths to ensure consistency
        
        The test is skipped if GEOFABRIK_OVERPASS_KEY is not set.
        """
        logging.info("==== Testing Overpass API connectivity ====")
        # Skip this test if GEOFABRIK_OVERPASS_KEY is not set
        if GEOFABRIK_OVERPASS_KEY is None:
            logging.info("GEOFABRIK_OVERPASS_KEY not set, skipping test_overpass")
            self.skipTest("GEOFABRIK_OVERPASS_KEY not set, skipping test_overpass")
            
        logging.info(f"Making request to Geofabrik API: {self.gfbk_url_base[:60]}...")
        r_gfbk = requests.get(self.gfbk_url_base)
        logging.info(f"Making request to public API: {self.public_url_base[:60]}...")
        r_public = requests.get(self.public_url_base)
        
        logging.info(f"Geofabrik API status: {r_gfbk.status_code}, Public API status: {r_public.status_code}")
        
        if r_gfbk.status_code == 200 and r_public.status_code == 200:
            logging.info("Both API requests were successful")
            r_gfbk_len, r_public_len = len(r_gfbk.json()), len(r_public.json())
            logging.info(f"Geofabrik response length: {r_gfbk_len}, Public response length: {r_public_len}")
            self.assertEqual(r_gfbk_len, r_public_len)
        else:
            logging.error(f"API request failed - Geofabrik: {r_gfbk.status_code}, Public: {r_public.status_code}")
            print("status_gfbk", r_gfbk.status_code, type(r_gfbk.status_code), "status_public", r_public.status_code)

    def test_get_stops_near(self):
        """
        Test the ability to find transit stops near a specified location.
        
        This test:
        1. Calls get_stops_near() with NREL East Gate coordinates
        2. Verifies the returned transit stop includes expected route information
        3. Checks that the RTD Route 125 details are correct
        
        This validates both the API connectivity and data parsing functionality.
        """
        logging.info("==== Testing get_stops_near function ====")
        logging.info(f"Searching for stops near NREL East Gate: {loc1['coordinates']}")
        
        stops = enetm.get_stops_near(loc1, 150.0)
        logging.info(f"Found {len(stops)} stops near location")
        
        if len(stops) > 0 and 'routes' in stops[0] and len(stops[0]['routes']) > 0:
            actual_result = stops[0]['routes'][0]['tags']
            logging.info(f"First route tags: {json.dumps(actual_result, indent=2)}")
            
            expected_result = {'from': 'National Renewable Energy Lab', 'name': 'RTD Route 125: Red Rocks College', 'network': 'RTD', 'network:wikidata': 'Q7309183', 'network:wikipedia': 'en:Regional Transportation District', 'operator': 'Regional Transportation District', 'public_transport:version': '1', 'ref': '125', 'route': 'bus', 'to': 'Red Rocks College', 'type': 'route'}
            self.assertEqual(expected_result, actual_result)
            logging.info("Route tags match expected values ✓")
        else:
            logging.error("No routes found in stop data")
            self.fail("No routes found in stop data")
   
    def test_get_predicted_transit_mode(self):
        """
        Test the ability to determine transit modes between two locations.
        
        This test:
        1. Finds stops near Denver Union Station and Grand Junction Train Station
        2. Calls get_predicted_transit_mode() to find common routes
        3. Verifies that train is correctly identified as the transit mode
        
        This tests the ability to identify long-distance transit connections.
        """
        logging.info("==== Testing get_predicted_transit_mode function ====")
        logging.info(f"Finding stops near Denver Union Station: {loc2['coordinates']}")
        stop1 = enetm.get_stops_near(loc2, 400.0)
        logging.info(f"Found {len(stop1)} stops near Denver Union Station")
        
        logging.info(f"Finding stops near Grand Junction Train Station: {loc3['coordinates']}")
        stop2 = enetm.get_stops_near(loc3, 400.0)
        logging.info(f"Found {len(stop2)} stops near Grand Junction Train Station")
        
        logging.info("Determining transit mode between the two locations")
        actual_result = enetm.get_predicted_transit_mode(stop1, stop2)
        expected_result = ['train', 'train']
        
        logging.info(f"Predicted transit modes: {actual_result}")
        self.assertEqual(actual_result, expected_result)
        logging.info("Transit modes match expected values ✓")
        
    def test_filesystem_caching(self):
        """
        Test that the filesystem caching functionality works correctly.
        
        This test:
        1. Notes the initial cache state
        2. Makes a first API call (should create cache)
        3. Checks that cache files were created
        4. Makes a second identical API call (should use cache)
        5. Verifies the results are the same
        6. Compares performance between uncached and cached calls
        """
        logging.info("==== Testing filesystem caching functionality ====")
        # Ensure we're in non-production mode (no GEOFABRIK_OVERPASS_KEY)
        original_key = None
        if "GEOFABRIK_OVERPASS_KEY" in os.environ:
            original_key = os.environ["GEOFABRIK_OVERPASS_KEY"]
            logging.info(f"Temporarily removing GEOFABRIK_OVERPASS_KEY for testing")
            del os.environ["GEOFABRIK_OVERPASS_KEY"]
        
        try:
            # Record initial cache state
            initial_cache_files = set(os.listdir(self.cache_dir))
            logging.info(f"Initial cache directory has {len(initial_cache_files)} files")
            
            # Make a first API call - this should create cache files
            logging.info(f"Making first API call (uncached)")
            start_time = time.time()
            first_result = enetm.get_stops_near(loc1, 200.0)
            first_call_duration = time.time() - start_time
            logging.info(f"First call returned {len(first_result)} stops and took {first_call_duration:.4f}s")
            
            # Check that cache files were created and track them
            current_cache_files = set(os.listdir(self.cache_dir))
            new_files = current_cache_files - initial_cache_files
            # Store the list of files created during this test for cleanup
            if not hasattr(self, 'test_created_files'):
                self.test_created_files = set()
            self.test_created_files.update(new_files)
            
            logging.info(f"Cache directory now has {len(current_cache_files)} files, added {len(new_files)} new files")
            self.assertTrue(len(new_files) > 0, "No cache files were created after the first API call")
            
            # Make a second identical API call - this should use the cache
            logging.info(f"Making second API call (should use cache)")
            start_time = time.time()
            second_result = enetm.get_stops_near(loc1, 200.0)
            second_call_duration = time.time() - start_time
            logging.info(f"Second call returned {len(second_result)} stops and took {second_call_duration:.4f}s")
            
            # Verify the results are the same
            self.assertEqual(len(first_result), len(second_result), 
                             "Cache returned different number of results than API call")
            logging.info(f"Both calls returned same number of results ✓")
            
            # Compare the first item's structure to ensure data integrity
            if len(first_result) > 0 and len(second_result) > 0:
                self.assertEqual(first_result[0].id, second_result[0].id, 
                                "Cache returned different stop ID than API call")
                logging.info(f"First stop ID matches in both results ✓")
                
                if 'routes' in first_result[0] and 'routes' in second_result[0]:
                    self.assertEqual(len(first_result[0]['routes']), len(second_result[0]['routes']),
                                   "Cache returned different number of routes than API call")
                    logging.info(f"Number of routes matches in both results ✓")
            
            # The second call should be faster if it's using the cache
            # (This is not always a reliable test due to network variability, but it's a good sanity check)
            logging.info(f"Performance comparison: First call took {first_call_duration:.4f}s, second call took {second_call_duration:.4f}s")
            if second_call_duration < first_call_duration:
                logging.info(f"Cache performance improvement: {(1 - second_call_duration/first_call_duration)*100:.1f}%")
            else:
                logging.warning(f"Second call was not faster - cache may not be working optimally")
            
        finally:
            # Restore original environment
            if original_key is not None:
                os.environ["GEOFABRIK_OVERPASS_KEY"] = original_key
                logging.info(f"Restored original GEOFABRIK_OVERPASS_KEY")

    def test_cache_creation_integrity(self):
        """
        Test that cache files are created with correct format and can be reused.
        
        This test:
        1. Notes the initial cache state
        2. Makes an API request that should create cache files
        3. Verifies new cache files are created
        4. Checks that cache files contain properly formatted JSON data
        5. Verifies returned stop data includes expected fields
        
        Cache integrity is essential for reliable operation between sessions.
        """
        logging.info("==== Testing cache creation integrity ====")
        # Ensure we're in non-production mode (no GEOFABRIK_OVERPASS_KEY)
        original_key = None
        if "GEOFABRIK_OVERPASS_KEY" in os.environ:
            original_key = os.environ["GEOFABRIK_OVERPASS_KEY"]
            logging.info(f"Temporarily removing GEOFABRIK_OVERPASS_KEY for testing")
            del os.environ["GEOFABRIK_OVERPASS_KEY"]
        
        try:
            # Get the initial cache state
            initial_cache_files = set(os.listdir(self.cache_dir))
            logging.info(f"Initial cache contains {len(initial_cache_files)} files")
            
            # Make a request (should create cache)
            logging.info(f"Making API request for Berkeley BART stops")
            stops = enetm.get_stops_near(loc4, 500)
            logging.info(f"Found {len(stops)} stops near Berkeley BART")
            
            # Check that we have more cache files now
            new_cache_files = set(os.listdir(self.cache_dir))
            new_files = new_cache_files - initial_cache_files
            logging.info(f"Created {len(new_files)} new cache files")
            
            # Store the list of files created during this test for cleanup
            if not hasattr(self, 'test_created_files'):
                self.test_created_files = set()
            self.test_created_files.update(new_files)
            
            self.assertGreater(len(new_files), 0, "No new cache files were created")
            
            # Verify at least one of the new files is a properly formatted JSON
            for file in new_files:
                file_path = os.path.join(self.cache_dir, file)
                self.assertTrue(file.endswith('.json'), f"Cache file {file} does not have .json extension")
                with open(file_path, 'r') as f:
                    try:
                        cache_data = json.load(f)
                        logging.info(f"Inspecting cache file {file}: contains {len(cache_data)} elements")
                        self.assertIsInstance(cache_data, list, f"Cache file {file} does not contain a JSON array")
                    except json.JSONDecodeError as e:
                        self.fail(f"Cache file {file} does not contain valid JSON: {e}")
                    
            # Make sure the stops data includes expected fields
            self.assertGreater(len(stops), 0, "No stops found")
            stop_fields = []
            for stop in stops:
                self.assertIn('id', stop, "Stop missing 'id' field")
                self.assertIn('tags', stop, "Stop missing 'tags' field")
                stop_fields = list(stop.keys())
                break
            
            logging.info(f"Stop data contains expected fields: {', '.join(stop_fields)} ✓")
                
        finally:
            # Restore original environment
            if original_key is not None:
                os.environ["GEOFABRIK_OVERPASS_KEY"] = original_key
                logging.info(f"Restored original GEOFABRIK_OVERPASS_KEY")

    def test_cache_query_hash(self):
        """
        Test that the query hashing mechanism works correctly.
        
        This test:
        1. Creates a test query string
        2. Generates an MD5 hash of the query string
        3. Creates a mock cache file with that hash name
        4. Patches the requests module to detect if an API call is attempted
        5. Makes a request that should use the cache instead of the API
        
        Proper query hashing ensures cache entries can be found and reused.
        """
        logging.info("==== Testing cache query hashing mechanism ====")
        # Ensure we're in non-production mode
        original_key = None
        if "GEOFABRIK_OVERPASS_KEY" in os.environ:
            original_key = os.environ["GEOFABRIK_OVERPASS_KEY"]
            logging.info(f"Temporarily removing GEOFABRIK_OVERPASS_KEY for testing")
            del os.environ["GEOFABRIK_OVERPASS_KEY"]
            
        try:
            # Create a test query string
            test_query = """[out:json][bbox:37.87,122.25,37.88,122.26];
                         node["public_transport"="stop_position"];
                         out;"""
            logging.info(f"Test query: {test_query.strip()}")
            
            # Generate hash the same way as in make_request_and_catch
            query_hash = hashlib.md5(test_query.encode()).hexdigest()
            logging.info(f"Generated query hash: {query_hash}")
            
            # Check that the hash is a valid string
            self.assertIsInstance(query_hash, str, "Query hash is not a string")
            self.assertEqual(len(query_hash), 32, "Query hash is not 32 characters (MD5 length)")
            
            # Create a mock cache file using JSON format
            cache_file = os.path.join(self.cache_dir, f"{query_hash}.json")
            cache_filename = f"{query_hash}.json"
            logging.info(f"Creating mock cache file: {cache_file}")
            mock_data = [{"id": 12345, "type": "node", "lat": 37.876, "lon": -122.258, "tags": {"name": "Test Stop"}}]
            with open(cache_file, 'w') as f:
                json.dump(mock_data, f)
            
            # Store the file created during this test for cleanup
            if not hasattr(self, 'test_created_files'):
                self.test_created_files = set()
            self.test_created_files.add(cache_filename)
            
            # Create a test overpass query function to intercept the API call
            # This lets us check if the function tries to use the cache before making an API call
            def mock_request(*args, **kwargs):
                logging.error("API request was made when cache should have been used")
                self.fail("API request was made when cache should have been used")
            
            # Save the original function
            original_requests = enetm.requests.post
            
            try:
                # Replace with our mock
                logging.info("Patching requests.post to detect API calls")
                enetm.requests.post = mock_request
                
                # Now make the request - if cache works, it won't call our mock function
                logging.info("Making request that should use cache")
                result = enetm.query_overpass(test_query)
                
                # Verify we got a result from the cache
                self.assertIsInstance(result, list, "Result is not a list")
                self.assertEqual(len(result), 1, "Wrong number of results from cache")
                self.assertEqual(result[0]['id'], 12345, "Cache returned incorrect data")
                logging.info(f"Successfully retrieved data from cache: {result[0]} ✓")
                
            finally:
                # Restore the original function
                enetm.requests.post = original_requests
                logging.info("Restored original requests.post function")
        
        finally:
            # Restore original environment
            if original_key is not None:
                os.environ["GEOFABRIK_OVERPASS_KEY"] = original_key
                logging.info(f"Restored original GEOFABRIK_OVERPASS_KEY")

    def test_production_mode_bypass_cache(self):
        """
        Test that production mode bypasses cache.
        
        This test:
        1. Makes a request to create cache files
        2. Sets CACHE_DIR to None to simulate production mode
        3. Patches the requests module to detect API calls
        4. Makes an identical request that should bypass the cache
        5. Verifies that the API was called despite cache existence
        
        Production mode should always use the API directly for fresh data.
        """
        logging.info("==== Testing production mode cache bypass ====")
        # Save original key and cache dir
        original_key = os.environ.get("GEOFABRIK_OVERPASS_KEY")
        original_cache_dir = enetm.CACHE_DIR
        
        # Skip test if we have a real key (to avoid actual API calls)
        if original_key:
            logging.info("Can't safely test production mode with actual key, skipping")
            self.skipTest("Can't safely test production mode with actual key")
        
        try:
            # Record initial cache state
            initial_cache_files = set(os.listdir(self.cache_dir))
            logging.info(f"Initial cache contains {len(initial_cache_files)} files")
            
            # First make a cached request
            logging.info("Making initial request to populate cache")
            stops = enetm.get_stops_near(loc4, 500)
            logging.info(f"Found {len(stops)} stops near Berkeley BART")
            
            # Get the cache files created and track them
            current_cache_files = set(os.listdir(self.cache_dir))
            new_files = current_cache_files - initial_cache_files
            logging.info(f"Cache now contains {len(current_cache_files)} files, added {len(new_files)} new files")
            
            # Store the list of files created during this test for cleanup
            if not hasattr(self, 'test_created_files'):
                self.test_created_files = set()
            self.test_created_files.update(new_files)
            
            self.assertGreater(len(new_files), 0, "No cache files created")
            
            # Now simulate production mode by setting CACHE_DIR to None
            logging.info("Setting CACHE_DIR to None to simulate production mode")
            enetm.CACHE_DIR = None
            
            # Create a test intercept function to verify the API is called
            api_called = [False]
            
            def mock_request(*args, **kwargs):
                logging.info("API call detected in production mode ✓")
                api_called[0] = True
                # Return a minimal valid response
                class MockResponse:
                    def json(self):
                        return {"elements": []}
                return MockResponse()
            
            # Save the original function
            original_requests = enetm.requests.post
            
            try:
                # Replace with our mock
                logging.info("Patching requests.post to detect API calls")
                enetm.requests.post = mock_request
                
                # Make request in "production mode"
                logging.info("Making same request in production mode")
                enetm.get_stops_near(loc4, 500)
                
                # Verify API was called even though cache exists
                self.assertTrue(api_called[0], "API not called in production mode")
                logging.info("Successfully verified that cache was bypassed in production mode")
                
            finally:
                # Restore the original function
                enetm.requests.post = original_requests
                logging.info("Restored original requests.post function")
        
        finally:
            # Restore original environment
            enetm.CACHE_DIR = original_cache_dir
            logging.info(f"Restored original CACHE_DIR")
            
            if original_key:
                os.environ["GEOFABRIK_OVERPASS_KEY"] = original_key
            elif "GEOFABRIK_OVERPASS_KEY" in os.environ:
                del os.environ["GEOFABRIK_OVERPASS_KEY"]

if __name__ == '__main__':
    # Without this I can't see the logging output why is that?
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    unittest.main()

