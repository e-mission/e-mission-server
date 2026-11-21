import os
import logging
import hashlib
import json
import unittest
from unittest.mock import patch, DEFAULT

import emission.analysis.classification.inference.mode.rule_engine as eacimr
import emission.net.ext_service.transit_matching.match_stops as enetm

original_get_prediction = eacimr.get_prediction
original_make_request_and_catch = enetm.make_request_and_catch

CACHE_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".overpass_cache")
os.makedirs(CACHE_DIR, exist_ok=True)

current_section_date = None

def mock_predict_mode(uuid):
    """
    Calls the real predict_mode function, but with mocked versions of get_prediction and
    make_request_and_catch that cache Overpass API responses to the filesystem.
    This is to avoid hitting the API repeatedly every time we run tests.
    """
    with patch('emission.analysis.classification.inference.mode.rule_engine.get_prediction') as get_prediction_patch:
        get_prediction_patch.side_effect = mock_get_prediction
        with patch('emission.net.ext_service.transit_matching.match_stops.make_request_and_catch') as make_request_and_catch_patch:
            make_request_and_catch_patch.side_effect = mock_make_request_and_catch
            eacimr.predict_mode(uuid)

def mock_get_prediction(i, section_entry):
    # Grab the date of the current section before calling the original function
    global current_section_date
    print('date of section is %s' % section_entry.data.start_fmt_time)
    current_section_date = section_entry.data.start_fmt_time[:10]
    return original_get_prediction(i, section_entry)

def mock_make_request_and_catch(overpass_query):
    # Create a hash based on the query
    query_hash = hashlib.md5(overpass_query.encode()).hexdigest()

    # Look in the cache for the date of the current section
    cache_file = os.path.join(CACHE_DIR, f"{current_section_date}.json")
    cache_file_exists = os.path.exists(cache_file)
    
    # If a cached response exists, use it
    if cache_file_exists:
        logging.info(f"Looking in {cache_file}")
        with open(cache_file, 'r') as f:
            cached_results = json.load(f)
        if query_hash in cached_results:
            return cached_results[query_hash]
    
    logging.warning(f"Didn't find {cache_file} in cache, making API request")
    # Else, make the request and cache the response before returning
    results = original_make_request_and_catch(overpass_query)
    
    # Add the results to the cache if it was successful
    if results != enetm.RETRY:
        if not cache_file_exists:
            cached_results = {}
        cached_results[query_hash] = results
        with open(cache_file, 'w') as f:
            json.dump(cached_results, f)
        logging.warning(f"Cached API response to {cache_file}")
    else:
        logging.warning(f"API request failed or returned no results, not caching response")

    return results
