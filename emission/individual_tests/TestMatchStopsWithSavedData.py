from __future__ import unicode_literals
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import
from future import standard_library
standard_library.install_aliases()
from builtins import *
import unittest
import os
import pandas as pd
import json
import csv
import pathlib
import logging
import attrdict as ad
import unittest.mock as mock

import emission.net.ext_service.transit_matching.match_stops as enetm
import emission.analysis.classification.inference.mode.rule_engine as eacimode

# Using the same locations as TestOverpass.py
loc1 = {'coordinates': [-105.16844103184974, 39.740428870224605]}  # NREL East Gate (bus stop)
loc2 = {'coordinates': [-105.00083982302972, 39.753710532185025]}  # Denver Union Station (train station)
loc3 = {'coordinates': [-108.57055213129632, 39.06472424640481]}   # Grand Junction Train Station

# Directory to store test data
TEST_DATA_DIR = "emission/tests/data/match_stops_test_data"
pathlib.Path(TEST_DATA_DIR).mkdir(parents=True, exist_ok=True)

class TestMatchStopsWithSavedData(unittest.TestCase):
    """
    This test file implements a "semi-mocked" approach to testing transit matching:
    - On first run, it makes real API calls to Overpass to fetch transit stops
    - It saves the results to CSV files for future test runs
    - Subsequent test runs use the saved data instead of calling the API again

    IMPORTANT: We still use mocking in our tests despite having saved data. Why?
    - The functions we're testing (like _get_transit_prediction) internally call get_stops_near()
    - Even with saved CSV data, these functions would still make real API calls if not mocked
    - So we mock get_stops_near() to return our saved data instead of letting it call the real API
    - This approach gives us real data while avoiding repeated API calls

    This is a two-phase approach:
    1. Data Collection Phase: Real API calls saved to CSV (first run)
    2. Test Execution Phase: Mocked API calls using saved data (all runs)

    For whoever is reading this -- I thought it'd be useful to have a reference for the following:
    - unittest.mock: https://docs.python.org/3/library/unittest.mock.html
    - OpenStreetMap Overpass API: https://wiki.openstreetmap.org/wiki/Overpass_API
    - OpenStreetMap Public Transport Schema: https://wiki.openstreetmap.org/wiki/Public_transport
    """

    def setUp(self):
        self.test_data_dir = TEST_DATA_DIR
        
    def save_stops_to_csv(self, stops, filename):
        """
        Save API response to CSV file for future test runs
        
        This handles serialization of complex nested objects by converting them to JSON strings.
        The stops data contains nested dictionaries which can't be directly stored in CSV format.
        
        This is part of the "Data Collection Phase" - we only do this once
        to avoid repeated API calls.
        """
        filepath = os.path.join(self.test_data_dir, filename)
        
        # Convert stops to a format that can be saved
        stops_data = []
        for stop in stops:
            stop_dict = dict(stop)
            # Convert routes to JSON string as CSV can't store complex structures
            if 'routes' in stop_dict:
                stop_dict['routes'] = json.dumps(stop_dict['routes'])
            # Convert tags to JSON string
            if 'tags' in stop_dict:
                stop_dict['tags'] = json.dumps(stop_dict['tags'])
            # Convert members to JSON string if present
            if 'members' in stop_dict:
                stop_dict['members'] = json.dumps(stop_dict['members'])
                
            stops_data.append(stop_dict)
        
        # Create DataFrame and save to CSV
        df = pd.DataFrame(stops_data)
        df.to_csv(filepath, index=False)
        logging.debug(f"Saved {len(stops_data)} stops to {filepath}")
        
    def load_stops_from_csv(self, filename):
        """
        Load stops from saved CSV file
        
        This handles deserialization of the previously saved JSON strings back into
        Python dictionaries and converts them to AttrDict objects for compatibility
        with the original code that expects attribute access.
        
        This is used in the "Test Execution Phase" to load previously saved data
        instead of making new API calls.
        """
        filepath = os.path.join(self.test_data_dir, filename)
        
        if not os.path.exists(filepath):
            return None
            
        df = pd.read_csv(filepath)
        stops = []
        
        for _, row in df.iterrows():
            row_dict = row.to_dict()
            
            # Convert JSON strings back to dictionaries
            if 'routes' in row_dict and isinstance(row_dict['routes'], str):
                try:
                    row_dict['routes'] = json.loads(row_dict['routes'])
                except json.JSONDecodeError:
                    row_dict['routes'] = []
                    
            if 'tags' in row_dict and isinstance(row_dict['tags'], str):
                try:
                    row_dict['tags'] = json.loads(row_dict['tags'])
                except json.JSONDecodeError:
                    row_dict['tags'] = {}
                    
            if 'members' in row_dict and isinstance(row_dict['members'], str):
                try:
                    row_dict['members'] = json.loads(row_dict['members'])
                except json.JSONDecodeError:
                    row_dict['members'] = []
            
            # Convert to AttrDict for compatibility with original code
            stops.append(ad.AttrDict(row_dict))
            
        return stops
    
    def get_stops_for_test(self, loc, distance, filename):
        """
        Get stops from CSV file or Overpass API if file doesn't exist
        
        This is the key function that implements the "call once, save for later" pattern.
        It first tries to load data from CSV, and only calls the real API if data doesn't exist.
        
        This function bridges the Data Collection Phase and the Test Execution Phase:
        - First run: Makes API calls and saves results (Collection Phase)
        - Subsequent runs: Uses saved data without API calls (Execution Phase)
        
        Parameters:
        - loc: location coordinates dictionary with 'coordinates' key
        - distance: search radius in meters
        - filename: name of the CSV file to save/load data
        
        Returns:
        - A list of stop objects (AttrDict instances)
        """
        # Try to load from CSV first
        stops = self.load_stops_from_csv(filename)
        
        # If no saved data, call API and save results
        if stops is None:
            stops = enetm.get_stops_near(loc, distance)
            self.save_stops_to_csv(stops, filename)
            
        return stops
    
    def test_get_stops_near(self):
        """
        Test get_stops_near by comparing with saved data
        
        This test verifies that the get_stops_near function correctly identifies
        transit stops near a given location.
        """
        # Get stops from CSV or API for location 1
        stops_near_loc1 = self.get_stops_for_test(loc1, 150.0, "loc1_stops.csv")
        
        # Verify the content - similar to original test
        self.assertTrue(len(stops_near_loc1) > 0, "Should find at least one stop")
        
        if len(stops_near_loc1) > 0 and 'routes' in stops_near_loc1[0] and len(stops_near_loc1[0]['routes']) > 0:
            self.assertEqual('bus', stops_near_loc1[0]['routes'][0]['tags']['route'], 
                            "First stop should be a bus stop")
    
    def test_get_predicted_transit_mode(self):
        """
        Test get_predicted_transit_mode using saved data
        
        This tests that the system correctly identifies the transit mode (train, bus, etc.)
        between two sets of transit stops.
        
        Note: This test directly calls get_predicted_transit_mode with our saved data,
        so it doesn't need to mock anything since we're not using any functions that
        would make API calls.
        """
        # Get stops for both locations
        stop1 = self.get_stops_for_test(loc2, 400.0, "loc2_stops.csv")
        stop2 = self.get_stops_for_test(loc3, 400.0, "loc3_stops.csv")
        
        # Run the function we want to test
        result = enetm.get_predicted_transit_mode(stop1, stop2)
        
        # Check that the result is as expected
        self.assertIsNotNone(result, "Should predict a transit mode")
        self.assertTrue(any('train' in mode.lower() for mode in result), 
                       "Should predict train as the transit mode")
                       
    def test_rule_engine_transit_prediction(self):
        """
        Test the _get_transit_prediction function in rule_engine
        
        This tests the rule engine's logic for predicting transit modes by
        mocking the get_stops_near function to return our saved data.
        
        IMPORTANT: Here's why we use mocking despite having saved data:
        - _get_transit_prediction internally calls get_stops_near()
        - If not mocked, it would make real API calls even though we have saved data
        - The mock redirects those calls to use our saved data instead
        """
        # Create a mock section entry
        section_entry = ad.AttrDict({
            'data': {
                'start_loc': loc2,  # Denver Union Station
                'end_loc': loc3,    # Grand Junction Train Station
                'speeds': [15, 20, 25, 30, 35]  # Speeds in m/s (typical train speeds), unless u are a bullet train
            }
        })
        
        # Load the stops from saved data
        start_stops = self.get_stops_for_test(loc2, 400.0, "loc2_stops.csv")
        end_stops = self.get_stops_for_test(loc3, 400.0, "loc3_stops.csv")
        
        # Mock get_stops_near to return our saved data
        # Without this mock, _get_transit_prediction would make real API calls which we dont really want
        # even though we already have the data saved to CSV
        with mock.patch('emission.net.ext_service.transit_matching.match_stops.get_stops_near') as mock_get_stops:
            # Configure the mock to return different values based on input
            mock_get_stops.side_effect = lambda loc, radius: start_stops if loc == section_entry.data.start_loc else end_stops
            
            # Run the function under test
            result = eacimode._get_transit_prediction(0, section_entry)
            
            # Verify the prediction
            self.assertIsNotNone(result, "Should predict a transit mode")
            self.assertEqual(result, "TRAIN", "Should predict TRAIN as the transit mode")
    
    def test_get_motorized_prediction(self):
        """
        Test get_motorized_prediction function
        
        This tests the system's ability to distinguish between transit (train/bus)
        and car trips based on nearby transit stops.
        
        Again, we use mocking to inject our saved data into get_motorized_prediction's
        internal calls to get_stops_near, avoiding actual API calls.
        """
        # Test with a train trip
        # This is the same as section_entry so maybe I ought to make these global vars
        train_section = ad.AttrDict({
            'data': {
                'start_loc': loc2,  # Denver Union Station
                'end_loc': loc3,    # Grand Junction Train Station
                'speeds': [15, 20, 25, 30, 35]
            }
        })
        
        # Test with a car trip (no transit stops)
        car_section = ad.AttrDict({
            'data': {
                'start_loc': {'coordinates': [-105.2, 39.8]},  # Random location with no transit stops
                'end_loc': {'coordinates': [-105.3, 39.9]},    # Random location with no transit stops
                'speeds': [10, 15, 20, 25]  # Speeds in m/s (typical car speeds) unless... you like to speed
            }
        })
        
        # Load saved data for train stops
        train_start_stops = self.get_stops_for_test(loc2, 400.0, "loc2_stops.csv")
        train_end_stops = self.get_stops_for_test(loc3, 400.0, "loc3_stops.csv")
        
        # For the car sections, we'll return empty lists since there are no transit stops
        car_stops = []
        
        # Mock get_stops_near to return our saved data
        # This is necessary because get_motorized_prediction indirectly calls get_stops_near
        # via _get_transit_prediction, which would trigger API calls without mocking
        with mock.patch('emission.net.ext_service.transit_matching.match_stops.get_stops_near') as mock_get_stops:
            # For train section
            mock_get_stops.side_effect = lambda loc, radius: \
                train_start_stops if loc == train_section.data.start_loc else \
                train_end_stops if loc == train_section.data.end_loc else \
                car_stops
            
            # Test train prediction
            train_result = eacimode.get_motorized_prediction(0, train_section)
            self.assertEqual(list(train_result.keys())[0], "TRAIN", "Should predict TRAIN for the train section")
            
            # Test car prediction
            mock_get_stops.side_effect = lambda loc, radius: car_stops
            car_result = eacimode.get_motorized_prediction(0, car_section)
            self.assertEqual(list(car_result.keys())[0], "CAR", "Should predict CAR for the car section")

if __name__ == '__main__':
    unittest.main() 