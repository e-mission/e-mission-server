import os
import logging
import hashlib
import json
import unittest
import emission.analysis.classification.inference.mode.rule_engine as eacimr
import emission.net.ext_service.transit_matching.match_stops as enetm

original_make_request_and_catch = enetm.make_request_and_catch

# Enable cache when using the public API
CACHE_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".overpass_cache")
os.makedirs(CACHE_DIR, exist_ok=True)


def mock_predict_mode(uuid):
    """
    Calls the real predict_mode function, but with a mocked version of make_request_and_catch
    that caches Overpass API responses to the filesystem.
    This is to avoid hitting the API repeatedly every time we run tests.
    """
    with unittest.mock.patch('emission.net.ext_service.transit_matching.match_stops.make_request_and_catch') as patch:
        patch.side_effect = mock_make_request_and_catch
        eacimr.predict_mode(uuid)
    eacimr.predict_mode(uuid)


def mock_make_request_and_catch(overpass_query):
    # Create a unique filename based on the query hash
    query_hash = hashlib.md5(overpass_query.encode()).hexdigest()
    cache_file = os.path.join(CACHE_DIR, f"{query_hash}.json")
    
    # If the cached response exists, use it
    if os.path.exists(cache_file):
        logging.info(f"Using cached response from {cache_file}")
        try:
            with open(cache_file, 'r') as f:
                all_results = json.load(f)
            return all_results
        except Exception as e:
            logging.warning(f"Error reading cache file: {e}, falling back to API")
    else:
        logging.warning(f"No cached response found for {overpass_query}, making API request")
    
    # Else, make the request and cache the response before returning
    all_results = original_make_request_and_catch(overpass_query)
    
    # Cache the results if it was successful
    if all_results and all_results != enetm.RETRY:
        try:
            with open(cache_file, 'w') as f:
                json.dump(all_results, f)
            logging.info(f"Cached API response to {cache_file}")
        except Exception as e:
            logging.warning(f"Error caching response: {e}")
    
    return all_results
