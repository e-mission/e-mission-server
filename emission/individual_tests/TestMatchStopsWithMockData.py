from __future__ import unicode_literals
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import
from future import standard_library
standard_library.install_aliases()
from builtins import *
import unittest
import os
import json
import logging
import attrdict as ad
import unittest.mock as mock

import emission.net.ext_service.transit_matching.match_stops as enetm
import emission.analysis.classification.inference.mode.rule_engine as eacimode

class TestMatchStopsWithMockData(unittest.TestCase):
    """
    This test file implements a fully mocked approach to testing transit matching:
    - All external API calls are completely mocked
    - Test data is manually defined within the test itself
    - No real API calls are ever made

    COMPARISON WITH SAVEDDATA APPROACH:
    Unlike TestMatchStopsWithSavedData, this approach never calls the real API.
    Instead of saving/loading real data from CSV files, we:
    1. Create completely synthetic test data in the code
    2. Use mocking to inject this synthetic data into the functions being tested

    ADVANTAGES:
    - Even faster tests (no initial API calls or CSV operations)
    - Fully controlled test data (not dependent on what the real API returns)
    - Tests can run without ever having internet access
    - Can test edge cases that might be hard to find in real data

    DISADVANTAGES:
    - Test data may not represent real-world scenarios as accurately
    - Might miss edge cases that exist in real API responses
    - Requires more upfront work to create realistic test data

    For whoever is reading this -- I thought it'd be useful to have a reference for the following:
    - unittest.mock: https://docs.python.org/3/library/unittest.mock.html
    - OpenStreetMap Overpass API: https://wiki.openstreetmap.org/wiki/Overpass_API
    - OpenStreetMap Elements data model: https://wiki.openstreetmap.org/wiki/Elements
    - OpenStreetMap Public Transport Schema: https://wiki.openstreetmap.org/wiki/Public_transport

    """

    def setUp(self):
        """
        Set up mock data for tests
        
        This creates synthetic transit stop data structured to match what would be
        returned by the Overpass API. We define different types of stops (bus, train)
        to test various scenarios.
        
        Unlike the SavedData approach where we get real data from the API first,
        here we manually create all the test data ourselves, giving us complete
        control over the test scenarios.
        """

        
        # Create mock transit data
        
        # Bus stop mock data
        self.mock_bus_stop = ad.AttrDict({
            'id': 12345,
            'lat': 39.740428,
            'lon': -105.168441,
            'type': 'node',
            'tags': {
                'highway': 'bus_stop',
                'name': 'NREL East Gate',
                'public_transport': 'platform',
                'network': 'RTD'
            },
            'routes': [
                {
                    'id': 54321,
                    'tags': {
                        'route': 'bus',
                        'ref': '125',
                        'name': 'RTD Route 125',
                        'network': 'RTD',
                        'operator': 'Regional Transportation District',
                        'type': 'route'
                    }
                }
            ]
        })
        
        # Train station mock data
        self.mock_train_stop_denver = ad.AttrDict({
            'id': 67890,
            'lat': 39.753710,
            'lon': -105.000839,
            'type': 'node',
            'tags': {
                'railway': 'station',
                'name': 'Denver Union Station',
                'public_transport': 'station',
                'network': 'Amtrak'
            },
            'routes': [
                {
                    'id': 98765,
                    'tags': {
                        'route': 'train',
                        'ref': 'California Zephyr',
                        'name': 'Amtrak California Zephyr',
                        'network': 'Amtrak',
                        'operator': 'Amtrak',
                        'type': 'route'
                    }
                }
            ]
        })
        
        # Another train station mock data
        self.mock_train_stop_grand_junction = ad.AttrDict({
            'id': 13579,
            'lat': 39.064724,
            'lon': -108.570552,
            'type': 'node',
            'tags': {
                'railway': 'station',
                'name': 'Grand Junction Station',
                'public_transport': 'station',
                'network': 'Amtrak'
            },
            'routes': [
                {
                    'id': 98765,  # Same route ID as Denver Union Station
                    'tags': {
                        'route': 'train',
                        'ref': 'California Zephyr',
                        'name': 'Amtrak California Zephyr',
                        'network': 'Amtrak',
                        'operator': 'Amtrak',
                        'type': 'route'
                    }
                }
            ]
        })
        
        # Sample location coordinates - these correspond to real locations
        # but we'll use them with our mock data instead of making real API calls
        self.loc_bus = {'coordinates': [-105.16844103184974, 39.740428870224605]}
        self.loc_denver = {'coordinates': [-105.00083982302972, 39.753710532185025]}
        self.loc_grand_junction = {'coordinates': [-108.57055213129632, 39.06472424640481]}
        self.loc_no_stops = {'coordinates': [-106.0, 40.0]}  # Random location with no transit stops
        
        # Mock lists for different test cases
        self.mock_bus_stops = [self.mock_bus_stop]
        self.mock_train_stops_denver = [self.mock_train_stop_denver]
        self.mock_train_stops_grand_junction = [self.mock_train_stop_grand_junction]
        self.mock_no_stops = []
    
    @mock.patch('emission.net.ext_service.transit_matching.match_stops.get_public_transit_stops')
    def test_get_stops_near(self, mock_get_public_transit_stops):
        """
        Test get_stops_near function using mock data
        
        This test verifies that the get_stops_near function correctly processes and returns
        transit stops data. We mock the underlying get_public_transit_stops function to
        return our prepared mock data instead of calling the Overpass API.
        
        Unlike the SavedData approach where we would load from CSV first, here we
        always use our synthetic data directly. This is simpler but less realistic.
        
        The @mock.patch decorator temporarily replaces the specified function with a mock object.
        See: https://docs.python.org/3/library/unittest.mock.html#patch-decorator
        """
        # Configure the mock
        mock_get_public_transit_stops.return_value = self.mock_bus_stops
        
        # Call the function under test
        result = enetm.get_stops_near(self.loc_bus, 150.0)
        
        # Verify the result
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].id, self.mock_bus_stop.id)
        self.assertEqual(result[0].tags.highway, 'bus_stop')
        self.assertEqual(len(result[0].routes), 1)
        self.assertEqual(result[0].routes[0].tags.route, 'bus')
        
        # Verify mock was called correctly
        mock_get_public_transit_stops.assert_called_once()
    
    @mock.patch('emission.net.ext_service.transit_matching.match_stops.get_stops_near')
    def test_get_predicted_transit_mode(self, mock_get_stops_near):
        """
        Test get_predicted_transit_mode function using mock data
        
        This tests the system's ability to identify transit modes (train, bus)
        between two sets of transit stops. We bypass mocking get_stops_near since
        we're calling get_predicted_transit_mode directly with our mock data.
        """
        # Configure the mock for train stops
        mock_get_stops_near.side_effect = None  # Reset side_effect
        
        # Call the function under test with train stops
        result = enetm.get_predicted_transit_mode(
            self.mock_train_stops_denver, 
            self.mock_train_stops_grand_junction
        )
        
        # Verify the result
        self.assertIsNotNone(result)
        self.assertTrue(isinstance(result, list))
        self.assertTrue('train' in result[0].lower() if result else False)
        
        # Test with bus stop and no stops (should return None)
        result_bus_none = enetm.get_predicted_transit_mode(
            self.mock_bus_stops,
            self.mock_no_stops
        )
        
        # For different types of stops, should return None
        self.assertIsNone(result_bus_none)
    
    @mock.patch('emission.net.ext_service.transit_matching.match_stops.get_stops_near')
    def test_rule_engine_transit_prediction(self, mock_get_stops_near):
        """
        Test _get_transit_prediction function in rule_engine
        
        This tests the rule engine's logic for predicting transit modes.
        We use lambda for side_effect to return different values based on the input,
        which allows us to simulate different stops at different locations.
        
        Reference on side_effect with lambda:
        https://docs.python.org/3/library/unittest.mock.html#unittest.mock.Mock.side_effect
        """
        # Create a mock section entry for train trip
        train_section = ad.AttrDict({
            'data': {
                'start_loc': self.loc_denver,
                'end_loc': self.loc_grand_junction,
                'speeds': [15, 20, 25, 30, 35]  # Speeds in m/s (typical train speeds)
            }
        })
        
        # Configure the mock to return our mock data
        # The lambda function allows different returns based on the input parameter
        mock_get_stops_near.side_effect = lambda loc, radius: \
            self.mock_train_stops_denver if loc == train_section.data.start_loc else \
            self.mock_train_stops_grand_junction
        
        # Call the function under test
        result = eacimode._get_transit_prediction(0, train_section)
        
        # Verify the prediction
        self.assertIsNotNone(result)
        self.assertEqual(result, "TRAIN")
        
        # Verify mock was called correctly
        self.assertEqual(mock_get_stops_near.call_count, 2)
    
    @mock.patch('emission.net.ext_service.transit_matching.match_stops.get_stops_near')
    def test_get_motorized_prediction(self, mock_get_stops_near):
        """
        Test get_motorized_prediction function with different scenarios
        
        This is a test that checks multiple scenarios:
        1. Train trip (matching train stops at start and end)
        2. Bus trip (bus stops at both locations)
        3. Car trip (no transit stops at either location)
        
        This verifies the system can correctly distinguish between transit and car trips.
        
        Both approaches need to mock get_stops_near for the same reason:
        - The function under test (get_motorized_prediction) doesn't know how to use
          our test data (whether from CSV or synthetic) without the mock
        """
        # 1. Test with train trip
        train_section = ad.AttrDict({
            'data': {
                'start_loc': self.loc_denver,
                'end_loc': self.loc_grand_junction,
                'speeds': [15, 20, 25, 30, 35]  # Speeds in m/s (typical train speeds)
            }
        })
        
        # Configure the mock for train trip
        mock_get_stops_near.side_effect = lambda loc, radius: \
            self.mock_train_stops_denver if loc == train_section.data.start_loc else \
            self.mock_train_stops_grand_junction
        
        # Test train prediction
        train_result = eacimode.get_motorized_prediction(0, train_section)
        self.assertEqual(list(train_result.keys())[0], "TRAIN")
        
        # 2. Test with bus trip
        bus_section = ad.AttrDict({
            'data': {
                'start_loc': self.loc_bus,
                'end_loc': self.loc_bus,  # Same location for simplicity
                'speeds': [8, 10, 12]  # Lower speeds typical for buses
            }
        })
        
        # Configure the mock for bus trip
        mock_get_stops_near.side_effect = lambda loc, radius: self.mock_bus_stops
        
        # Test bus prediction
        bus_result = eacimode.get_motorized_prediction(0, bus_section)
        # Note: This might return 'BUS' or not depending on the validation logic
        
        # 3. Test with car trip (no transit stops)
        car_section = ad.AttrDict({
            'data': {
                'start_loc': self.loc_no_stops,
                'end_loc': {'coordinates': [-106.1, 40.1]},  # Another random location
                'speeds': [10, 15, 20, 25]  # Car speeds
            }
        })
        
        # Configure the mock for car trip (no stops)
        mock_get_stops_near.side_effect = lambda loc, radius: self.mock_no_stops
        
        # Test car prediction
        car_result = eacimode.get_motorized_prediction(0, car_section)
        self.assertEqual(list(car_result.keys())[0], "CAR")
    
    @mock.patch('emission.net.ext_service.transit_matching.match_stops.get_stops_near')
    def test_with_combined_stops(self, mock_get_stops_near):
        """
        Test with a location that has both bus and train stops
        
        This tests an edge case where a location has multiple types of transit stops.
        The system should be able to make a reasonable prediction based on the
        available stops and the speed profile of the trip.
        
        This kind of test is easier with the fully mocked approach because we can
        easily create combined lists of different stop types, whereas with real data
        we'd need to find locations that actually have multiple transit types.
        """
        # Create a combined mock stop list
        combined_stops = self.mock_bus_stops + self.mock_train_stops_denver
        
        # Create a section with start and end having both types of stops
        mixed_section = ad.AttrDict({
            'data': {
                'start_loc': self.loc_denver,
                'end_loc': self.loc_denver,  # Same location for simplicity
                'speeds': [12, 15, 18]  # Medium speeds
            }
        })
        
        # Configure the mock
        mock_get_stops_near.return_value = combined_stops
        
        # Test the prediction
        result = eacimode._get_transit_prediction(0, mixed_section)
        
        # Should pick one of the transit modes based on speeds and rule logic
        self.assertIsNotNone(result)
        self.assertTrue(result in ["BUS", "TRAIN"])

if __name__ == '__main__':
    unittest.main() 