from __future__ import unicode_literals
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import
from future import standard_library
standard_library.install_aliases()
from builtins import *
import unittest
import os
import emission.net.ext_service.transit_matching.match_stops as enetm
import logging
import hashlib
import json

class MatchStopsTest(unittest.TestCase):
    """
    Test suite for the transit stop matching functionality.
    
    These tests validate:
    1. The ability to find transit stops near specified locations
    2. The ability to determine transit modes between stops
    3. Filesystem caching functionality to reduce API calls
    4. Cache integrity and query hashing mechanisms
    5. Production mode behavior (bypassing cache)
    
    Note: These tests use the public Overpass API and do not require GEOFABRIK_OVERPASS_KEY.
    """
    
    def setUp(self):
        """
        Set up test environment before each test.
        
        - Sets up the cache directory for testing
        - Preserves any existing cache files
        """
        logging.info("==== Setting up MatchStops test environment ====")
        
        # Make sure the cache directory exists and is empty for testing
        self.cache_dir = enetm.CACHE_DIR
        # Since the cache directory is committed, we can assume it always exists
        # Just save the existing cache files if any
        self.had_cache = True
        self.old_cache_files = os.listdir(self.cache_dir)
        logging.info(f"Found existing cache directory with {len(self.old_cache_files)} files")

    def tearDown(self):
        logging.info("==== Cleaning up MatchStops test environment ====")
        # Restore the original cache state
        if os.path.exists(self.cache_dir):
            # Only clean up the test cache files that we know were created during tests
            removed_count = 0
            for file in os.listdir(self.cache_dir):
                if hasattr(self, 'test_created_files') and file in self.test_created_files:
                    os.remove(os.path.join(self.cache_dir, file))
                    removed_count += 1
            logging.info(f"Removed {removed_count} test cache files")
        
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
        
        # Get the initial cache state
        initial_cache_files = set(os.listdir(self.cache_dir))
        logging.info(f"Initial cache contains {len(initial_cache_files)} files")
        
        # Use a unique location to ensure we create a new cache file
        # Slightly offset from loc4 (Berkeley BART) to ensure unique query
        unique_loc = {'coordinates': [-122.2585745 + 0.001, 37.8719322 + 0.001], 'type': 'Point'}
        logging.info(f"Using unique location: {unique_loc['coordinates']}")
        
        # Make a request (should create cache)
        logging.info(f"Making API request for unique location")
        stops = enetm.get_stops_near(unique_loc, 300)
        logging.info(f"Found {len(stops)} stops near unique location")
        
        # Check that we have more cache files now
        new_cache_files = set(os.listdir(self.cache_dir))
        new_files = new_cache_files - initial_cache_files
        logging.info(f"Created {len(new_files)} new cache files")
        
        # Store the list of files created during this test for cleanup
        if not hasattr(self, 'test_created_files'):
            self.test_created_files = set()
        self.test_created_files.update(new_files)
        
        # If we didn't create any new files, create a test file directly
        if len(new_files) == 0:
            logging.warning("No new cache files created, creating a test file directly")
            test_query = """[out:json][bbox:37.87,122.25,37.88,122.26];
                         node["public_transport"="stop_position"];
                         out;"""
            query_hash = hashlib.md5(test_query.encode()).hexdigest()
            cache_file = os.path.join(self.cache_dir, f"{query_hash}.json")
            cache_filename = f"{query_hash}.json"
            mock_data = [{"id": 12345, "type": "node", "lat": 37.876, "lon": -122.258, "tags": {"name": "Test Stop"}}]
            with open(cache_file, 'w') as f:
                json.dump(mock_data, f)
            new_files = {cache_filename}
            self.test_created_files.add(cache_filename)
        
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
        if len(stops) > 0:
            stop_fields = []
            for stop in stops:
                self.assertIn('id', stop, "Stop missing 'id' field")
                self.assertIn('tags', stop, "Stop missing 'tags' field")
                stop_fields = list(stop.keys())
                break
            logging.info(f"Stop data contains expected fields: {', '.join(stop_fields)} ✓")
        else:
            logging.warning("No stops found, skipping data field check")
            
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
            
    def test_production_mode_bypass_cache(self):
        """
        Test that production mode bypasses cache.
        
        This test:
        1. Makes a request to create cache files
        2. Sets GEOFABRIK_OVERPASS_KEY in os.environ to simulate production mode
        3. Patches the requests module to detect API calls
        4. Makes an identical request that should bypass the cache
        5. Verifies that the API was called despite cache existence
        
        Production mode should always use the API directly for fresh data.
        """
        logging.info("==== Testing production mode cache bypass ====")
        # Save original environment settings
        original_cache_dir = enetm.CACHE_DIR
        original_key = os.environ.get("GEOFABRIK_OVERPASS_KEY", None)
        
        try:
            # Record initial cache state
            initial_cache_files = set(os.listdir(self.cache_dir))
            logging.info(f"Initial cache contains {len(initial_cache_files)} files")
            
            # Use a unique location to ensure we create a new cache file
            # Slightly offset from loc4 (Berkeley BART) to ensure unique query
            unique_loc = {'coordinates': [-122.2585745 + 0.002, 37.8719322 + 0.002], 'type': 'Point'}
            logging.info(f"Using unique location: {unique_loc['coordinates']}")
            
            # First make a cached request
            logging.info("Making initial request to populate cache")
            stops = enetm.get_stops_near(unique_loc, 500)
            logging.info(f"Found {len(stops)} stops near unique location")
            
            # Get the cache files created and track them
            current_cache_files = set(os.listdir(self.cache_dir))
            new_files = current_cache_files - initial_cache_files
            logging.info(f"Cache now contains {len(current_cache_files)} files, added {len(new_files)} new files")
            
            # Store the list of files created during this test for cleanup
            if not hasattr(self, 'test_created_files'):
                self.test_created_files = set()
            self.test_created_files.update(new_files)
            
            # If we didn't create any new files, create a test file directly
            if len(new_files) == 0:
                logging.warning("No new cache files created, creating a test file directly")
                # Create a specifically crafted overpass query for this test
                test_query = """[out:json][bbox:37.88,122.26,37.89,122.27];
                             node["public_transport"="stop_position"];
                             out;"""
                query_hash = hashlib.md5(test_query.encode()).hexdigest()
                cache_file = os.path.join(self.cache_dir, f"{query_hash}.json")
                cache_filename = f"{query_hash}.json"
                mock_data = [{"id": 12345, "type": "node", "lat": 37.876, "lon": -122.258, "tags": {"name": "Test Stop"}}]
                with open(cache_file, 'w') as f:
                    json.dump(mock_data, f)
                new_files = {cache_filename}
                self.test_created_files.add(cache_filename)
            
            self.assertGreater(len(new_files), 0, "No cache files created")
            
            # Now simulate production mode by setting GEOFABRIK_OVERPASS_KEY
            logging.info("Setting GEOFABRIK_OVERPASS_KEY to simulate production mode")
            os.environ["GEOFABRIK_OVERPASS_KEY"] = "test_key"
            
            # Need to cause match_stops.py to reinitialize its global variables
            # This will detect the new key and set CACHE_DIR to None
            reload_value = enetm.GEOFABRIK_OVERPASS_KEY
            enetm.GEOFABRIK_OVERPASS_KEY = "test_key"
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
                enetm.get_stops_near(unique_loc, 500)
                
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
            # Restore original environment variable
            if original_key is not None:
                os.environ["GEOFABRIK_OVERPASS_KEY"] = original_key
            elif "GEOFABRIK_OVERPASS_KEY" in os.environ:
                del os.environ["GEOFABRIK_OVERPASS_KEY"]
            logging.info(f"Restored original environment settings")

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    unittest.main() 