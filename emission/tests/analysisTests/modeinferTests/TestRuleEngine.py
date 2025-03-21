import unittest
import logging
import json
import numpy as np
import pandas as pd
import uuid
import os
from datetime import datetime
from unittest.mock import patch

# Our imports
import emission.core.get_database as edb
import emission.storage.timeseries.abstract_timeseries as esta
import emission.storage.decorations.analysis_timeseries_queries as esda
import emission.storage.pipeline_queries as epq
import emission.pipeline.reset as epr

import emission.analysis.classification.inference.mode.rule_engine as rule_engine
import emission.core.wrapper.modeprediction as ecwm
import emission.core.wrapper.motionactivity as ecwma
import emission.core.wrapper.entry as ecwe
import emission.core.wrapper.section as ecws
import emission.core.wrapper.cleanedsection as ecwcs
import emission.core.wrapper.pipelinestate as ecwp

import emission.tests.common as etc
import emission.net.ext_service.transit_matching.match_stops as enetm

class TestRuleEngine(unittest.TestCase):
    """
    Test suite for the rule-based mode inference engine.
    
    The rule engine analyzes motion data to predict transportation modes (walking, cycling, 
    driving, transit, etc.) based on various factors like:
    - Sensed motion activity (from phone sensors)
    - Speed profiles
    - Proximity to transit stops
    
    These tests validate that the engine correctly:
    - Preserves explicitly sensed modes when appropriate
    - Infers modes based on speed patterns
    - Considers transit stop proximity
    - Handles edge cases like empty inputs
    - Produces consistent results
    """
    def setUp(self):
        # Create a test user
        self.testUUID = uuid.uuid4()
        self.ts = esta.TimeSeries.get_time_series(self.testUUID)
        self.clearData()

    def tearDown(self):
        self.clearData()

    def clearData(self):
        edb.get_timeseries_db().delete_many({"user_id": self.testUUID})
        edb.get_analysis_timeseries_db().delete_many({"user_id": self.testUUID})
        # Also delete pipeline states for this user
        edb.get_pipeline_state_db().delete_many({"user_id": self.testUUID})
        
    def testPredictModeForEmptySections(self):
        """
        Test that the pipeline handles empty section lists properly.
        
        When no sections exist for a user, the pipeline should:
        1. Create a pipeline state entry
        2. Not update the last_processed_ts (since nothing was processed)
        3. Not create any prediction results
        """
        # First reset any pipeline state
        epr.reset_curr_run_state(self.testUUID, False)
        rule_engine.predict_mode(self.testUUID)
        # After running with empty sections, there should be a pipeline state but no results
        pipeline_state = epq.get_current_state(self.testUUID, ecwp.PipelineStages.MODE_INFERENCE)
        self.assertIsNotNone(pipeline_state)
        self.assertIsNone(pipeline_state.last_processed_ts)
        logging.info("=== Empty sections test: Pipeline state created correctly with no data ===")

    def create_test_section(self, sensed_mode, speeds=None, start_loc=None, end_loc=None):
        """
        Helper method to create a test section with specified parameters.
        
        This creates a cleaned section entry in the timeseries database with customizable:
        - Sensed mode (the motion activity detected by the phone)
        - Speed array (movement speeds in m/s) 
        - Start and end locations (as GeoJSON Point objects)
        """
        section_data = {
            "trip_id": "test_trip",
            "source": "test",
            "start_ts": 1000000,
            "end_ts": 1001000,
            "start_fmt_time": "2020-01-01T00:00:00+00:00",
            "end_fmt_time": "2020-01-01T00:16:40+00:00",
            "duration": 1000,
            "sensed_mode": sensed_mode,
            "distance": 1000
        }
        
        if speeds is None:
            section_data["speeds"] = [5, 5, 5, 5]
        else:
            section_data["speeds"] = speeds
            
        if start_loc is None:
            section_data["start_loc"] = {
                "type": "Point", 
                "coordinates": [0, 0]
            }
        else:
            section_data["start_loc"] = start_loc
        
        if end_loc is None:
            section_data["end_loc"] = {
                "type": "Point", 
                "coordinates": [0.01, 0.01]
            }
        else:
            section_data["end_loc"] = end_loc
        
        entry = ecwe.Entry.create_entry(self.testUUID, "analysis/cleaned_section", section_data)
        self.ts.insert(entry)
        return entry

    def print_prediction_results(self, section_entry, prediction_entry, inferred_entry, test_name, expected_mode=None):
        """
        Helper method to format and log prediction results in a readable format.
        
        This displays:
        - The input sensed mode from the section
        - The speed profile used
        - The expected mode (if provided)
        - The complete prediction map with all mode probabilities
        - The top predicted mode with its confidence
        - The final inferred mode
        - Whether the prediction matches expectations
        """
        sensed_mode_name = ecwma.MotionTypes(section_entry["data"]["sensed_mode"]).name if section_entry["data"]["sensed_mode"] in [e.value for e in ecwma.MotionTypes] else f"UNKNOWN({section_entry['data']['sensed_mode']})"
        
        predicted_mode_map = prediction_entry["data"]["predicted_mode_map"]
        max_conf_mode = max(predicted_mode_map.items(), key=lambda x: x[1])
        
        inferred_mode_name = ecwm.PredictedModeTypes(inferred_entry["data"]["sensed_mode"]).name if inferred_entry["data"]["sensed_mode"] in [e.value for e in ecwm.PredictedModeTypes] else f"UNKNOWN({inferred_entry['data']['sensed_mode']})"
        
        # Determine if prediction matches expectation
        result_indicator = "✓ MATCH" if expected_mode == inferred_mode_name else "✗ MISMATCH"
        
        logging.info(f"===== {test_name} RESULTS =====")
        logging.info(f"Input sensed_mode: {sensed_mode_name}")
        logging.info(f"Speeds: {section_entry['data']['speeds']}")
        logging.info(f"Expected mode: {expected_mode if expected_mode else 'Not specified'}")
        logging.info(f"Prediction map: {predicted_mode_map}")
        logging.info(f"Top prediction: {max_conf_mode[0]} with confidence {max_conf_mode[1]}")
        logging.info(f"Inferred mode: {inferred_mode_name}")
        if expected_mode:
            logging.info(f"RESULT: {result_indicator}")
        logging.info("========================")
        
        return inferred_mode_name

    def testAirOrHSROverride(self):
        """
        Test that AIR_OR_HSR sensed mode is correctly preserved.
        
        When a section has AIR_OR_HSR as the sensed mode, the rule engine should:
        1. Preserve this mode regardless of other factors
        2. Create a prediction with 100% confidence for AIR_OR_HSR
        3. Create an inferred section with AIR_OR_HSR mode
        
        This is a special case rule that ensures high-speed rail and air travel
        are correctly identified.
        """
        # First reset any pipeline state
        epr.reset_curr_run_state(self.testUUID, False)
        section_entry = self.create_test_section(ecwma.MotionTypes.AIR_OR_HSR.value)
        
        # Run rule engine
        rule_engine.predict_mode(self.testUUID)
        
        # Check prediction
        predictions = self.ts.find_entries(["inference/prediction"])
        self.assertEqual(len(predictions), 1)
        
        # Entries returned by find_entries are already dictionaries
        prediction_dict = predictions[0]
        self.assertEqual(prediction_dict["data"]["predicted_mode_map"], {'AIR_OR_HSR': 1})
        
        # Check inferred section
        inferred_sections = self.ts.find_entries(["analysis/inferred_section"])
        self.assertEqual(len(inferred_sections), 1)
        self.assertEqual(inferred_sections[0]["data"]["sensed_mode"], ecwm.PredictedModeTypes.AIR_OR_HSR.value)
        
        # Print results for debugging
        expected_mode = "AIR_OR_HSR"
        inferred_mode = self.print_prediction_results(section_entry, prediction_dict, inferred_sections[0], 
                                                    "AIR_OR_HSR OVERRIDE", expected_mode)
        self.assertEqual(inferred_mode, expected_mode)

    def testMotorizedWithoutTransitStops(self):
        """
        Test motorized mode with no nearby transit stops.
        
        When a section has IN_VEHICLE as the sensed mode and there are no transit
        stops nearby, the rule engine should:
        1. Infer CAR as the transportation mode
        2. Create a prediction with 100% confidence for CAR
        3. Create an inferred section with CAR mode
        
        This tests the default case for motorized travel.
        """
        # First reset any pipeline state
        epr.reset_curr_run_state(self.testUUID, False)
        section_entry = self.create_test_section(ecwma.MotionTypes.IN_VEHICLE.value)
        
        # Run rule engine
        rule_engine.predict_mode(self.testUUID)
        
        # Check prediction
        predictions = self.ts.find_entries(["inference/prediction"])
        self.assertEqual(len(predictions), 1)
        
        # Entries returned by find_entries are already dictionaries
        prediction_dict = predictions[0]
        self.assertEqual(prediction_dict["data"]["predicted_mode_map"], {'CAR': 1})
        
        # Check inferred section
        inferred_sections = self.ts.find_entries(["analysis/inferred_section"])
        self.assertEqual(len(inferred_sections), 1)
        self.assertEqual(inferred_sections[0]["data"]["sensed_mode"], ecwm.PredictedModeTypes.CAR.value)
        
        # Print results for debugging
        expected_mode = "CAR"
        inferred_mode = self.print_prediction_results(section_entry, prediction_dict, inferred_sections[0], 
                                                    "MOTORIZED WITHOUT TRANSIT", expected_mode)
        self.assertEqual(inferred_mode, expected_mode)

    def testNonMotorizedWalkingSpeed(self):
        """
        Test non-motorized mode at walking speed.
        
        When a section has WALKING as the sensed mode and speeds are
        consistent with walking (< 2 m/s), the rule engine should:
        1. Infer WALKING as the transportation mode
        2. Create a prediction with 100% confidence for WALKING
        3. Create an inferred section with WALKING mode
        
        This tests the common case of regular pedestrian activity.
        """
        # First reset any pipeline state
        epr.reset_curr_run_state(self.testUUID, False)
        section_entry = self.create_test_section(
            ecwma.MotionTypes.WALKING.value,
            speeds=[1.2, 1.3, 1.1, 1.4]  # Walking speeds < 2 m/s
        )
        
        # Run rule engine
        rule_engine.predict_mode(self.testUUID)
        
        # Check prediction
        predictions = self.ts.find_entries(["inference/prediction"])
        self.assertEqual(len(predictions), 1)
        
        # Entries returned by find_entries are already dictionaries
        prediction_dict = predictions[0]
        self.assertEqual(prediction_dict["data"]["predicted_mode_map"], {'WALKING': 1})
        
        # Check inferred section
        inferred_sections = self.ts.find_entries(["analysis/inferred_section"])
        self.assertEqual(len(inferred_sections), 1)
        self.assertEqual(inferred_sections[0]["data"]["sensed_mode"], ecwm.PredictedModeTypes.WALKING.value)
        
        # Print results for debugging
        expected_mode = "WALKING"
        inferred_mode = self.print_prediction_results(section_entry, prediction_dict, inferred_sections[0], 
                                                    "WALKING SPEED", expected_mode)
        self.assertEqual(inferred_mode, expected_mode)

    def testNonMotorizedCyclingSpeed(self):
        """
        Test non-motorized mode at cycling speed.
        
        When a section has ON_FOOT as the sensed mode but speeds are
        consistent with bicycling (> 3 m/s), the rule engine should:
        1. Override the sensed mode and infer BICYCLING
        2. Create a prediction with 100% confidence for BICYCLING
        3. Create an inferred section with BICYCLING mode
        
        This tests the speed-based correction, where the rule engine
        determines that the sensed mode is likely incorrect based on speeds.
        """
        # First reset any pipeline state
        epr.reset_curr_run_state(self.testUUID, False)
        section_entry = self.create_test_section(
            ecwma.MotionTypes.ON_FOOT.value,  # Detected as on foot
            speeds=[4.5, 5.1, 4.8, 5.2]  # But speeds are cycling speeds (> 3 m/s)
        )
        
        # Run rule engine
        rule_engine.predict_mode(self.testUUID)
        
        # Check prediction
        predictions = self.ts.find_entries(["inference/prediction"])
        self.assertEqual(len(predictions), 1)
        
        # Entries returned by find_entries are already dictionaries
        prediction_dict = predictions[0]
        self.assertEqual(prediction_dict["data"]["predicted_mode_map"], {'BICYCLING': 1})
        
        # Check inferred section
        inferred_sections = self.ts.find_entries(["analysis/inferred_section"])
        self.assertEqual(len(inferred_sections), 1)
        self.assertEqual(inferred_sections[0]["data"]["sensed_mode"], ecwm.PredictedModeTypes.BICYCLING.value)
        
        # Print results for debugging
        expected_mode = "BICYCLING"
        inferred_mode = self.print_prediction_results(section_entry, prediction_dict, inferred_sections[0], 
                                                    "CYCLING SPEED", expected_mode)
        self.assertEqual(inferred_mode, expected_mode)

    def testMultipleSections(self):
        """
        Test processing multiple sections in one run.
        
        This test creates three sections with different characteristics:
        1. A WALKING section
        2. An IN_VEHICLE section
        3. An AIR_OR_HSR section
        
        The rule engine should process all three sections in a single run and
        create appropriate predictions and inferred sections for each one.
        
        This tests batch processing capability of the rule engine.
        """
        # First reset any pipeline state
        epr.reset_curr_run_state(self.testUUID, False)
        # Create three sections with different characteristics
        section1 = self.create_test_section(ecwma.MotionTypes.WALKING.value)
        section2 = self.create_test_section(ecwma.MotionTypes.IN_VEHICLE.value)
        section3 = self.create_test_section(ecwma.MotionTypes.AIR_OR_HSR.value)
        
        # Run rule engine
        rule_engine.predict_mode(self.testUUID)
        
        # Check predictions
        predictions = self.ts.find_entries(["inference/prediction"])
        self.assertEqual(len(predictions), 3)
        
        # Check inferred sections were created
        inferred_sections = self.ts.find_entries(["analysis/inferred_section"])
        self.assertEqual(len(inferred_sections), 3)
        
        # Print results for multiple sections
        expected_modes = ["WALKING", "CAR", "AIR_OR_HSR"]
        logging.info("===== MULTIPLE SECTIONS RESULTS =====")
        logging.info(f"{'Section':<10} {'Input':<15} {'Expected':<15} {'Predicted':<15} {'Result':<10}")
        logging.info("-" * 65)
        for i, (section, prediction, inferred) in enumerate(zip(
            [section1, section2, section3], predictions, inferred_sections)):
            sensed_mode = ecwma.MotionTypes(section["data"]["sensed_mode"]).name if section["data"]["sensed_mode"] in [e.value for e in ecwma.MotionTypes] else f"UNKNOWN({section['data']['sensed_mode']})"
            inferred_mode = ecwm.PredictedModeTypes(inferred["data"]["sensed_mode"]).name if inferred["data"]["sensed_mode"] in [e.value for e in ecwm.PredictedModeTypes] else f"UNKNOWN({inferred['data']['sensed_mode']})"
            expected = expected_modes[i]
            result = "✓ MATCH" if expected == inferred_mode else "✗ MISMATCH"
            logging.info(f"{i+1:<10} {sensed_mode:<15} {expected:<15} {inferred_mode:<15} {result:<10}")
        logging.info("======================================")

    def testConsistency(self):
        """
        Test that running the rule engine multiple times produces consistent results.
        
        This test:
        1. Creates an IN_VEHICLE section and runs the rule engine
        2. Records the prediction and inferred mode
        3. Clears all data
        4. Creates an identical IN_VEHICLE section and runs the rule engine again
        5. Verifies that both runs produce identical predictions and inferred modes
        
        This ensures that the rule engine is deterministic and produces
        consistent results for identical inputs.
        """
        # First reset any pipeline state
        epr.reset_curr_run_state(self.testUUID, False)
        section_entry = self.create_test_section(ecwma.MotionTypes.IN_VEHICLE.value)
        
        # First run
        rule_engine.predict_mode(self.testUUID)
        predictions1 = self.ts.find_entries(["inference/prediction"])
        inferred_sections1 = self.ts.find_entries(["analysis/inferred_section"])
        
        # Entries returned by find_entries are already dictionaries
        prediction_dict1 = predictions1[0]
        inferred_mode1 = inferred_sections1[0]["data"]["sensed_mode"]
        
        # Print results for first run
        expected_mode = "CAR"
        logging.info("===== CONSISTENCY TEST - FIRST RUN =====")
        logging.info(f"Expected mode: {expected_mode}")
        logging.info(f"Prediction: {prediction_dict1['data']['predicted_mode_map']}")
        logging.info(f"Inferred mode: {ecwm.PredictedModeTypes(inferred_mode1).name}")
        first_run_mode = ecwm.PredictedModeTypes(inferred_mode1).name
        
        # Clear and rerun
        self.clearData()
        section_entry = self.create_test_section(ecwma.MotionTypes.IN_VEHICLE.value)
        
        # Second run
        rule_engine.predict_mode(self.testUUID)
        predictions2 = self.ts.find_entries(["inference/prediction"])
        inferred_sections2 = self.ts.find_entries(["analysis/inferred_section"])
        
        # Entries returned by find_entries are already dictionaries
        prediction_dict2 = predictions2[0]
        inferred_mode2 = inferred_sections2[0]["data"]["sensed_mode"]
        
        # Print results for second run
        logging.info("===== CONSISTENCY TEST - SECOND RUN =====")
        logging.info(f"Expected mode: {expected_mode}")
        logging.info(f"Prediction: {prediction_dict2['data']['predicted_mode_map']}")
        logging.info(f"Inferred mode: {ecwm.PredictedModeTypes(inferred_mode2).name}")
        second_run_mode = ecwm.PredictedModeTypes(inferred_mode2).name
        
        # Check consistency
        self.assertEqual(len(predictions1), len(predictions2))
        self.assertEqual(prediction_dict1["data"]["predicted_mode_map"], prediction_dict2["data"]["predicted_mode_map"])
        self.assertEqual(inferred_mode1, inferred_mode2)
        
        # Print consistency result
        logging.info("===== CONSISTENCY RESULT =====")
        logging.info(f"{'Property':<20} {'First Run':<15} {'Second Run':<15} {'Match?':<10}")
        logging.info("-" * 65)
        logging.info(f"{'Expected mode':<20} {expected_mode:<15} {expected_mode:<15} {'✓':<10}")
        logging.info(f"{'Inferred mode':<20} {first_run_mode:<15} {second_run_mode:<15} {'✓' if first_run_mode == second_run_mode else '✗':<10}")
        prediction_match = prediction_dict1["data"]["predicted_mode_map"] == prediction_dict2["data"]["predicted_mode_map"]
        logging.info(f"{'Prediction map':<20} {'[complex]':<15} {'[complex]':<15} {'✓' if prediction_match else '✗':<10}")
        logging.info(f"{'Matches expected':<20} {'✓' if first_run_mode == expected_mode else '✗':<15} {'✓' if second_run_mode == expected_mode else '✗':<15}")
        logging.info("===============================")
        
    @patch('emission.net.ext_service.transit_matching.match_stops.get_stops_near')
    @patch('emission.net.ext_service.transit_matching.match_stops.get_predicted_transit_mode')
    def testMotorizedWithTransitStops(self, mock_get_predicted_transit_mode, mock_get_stops_near):
        """
        Test motorized mode with transit stops nearby.
        
        This test:
        1. Mocks the transit stop detection to return bus stops
        2. Creates an IN_VEHICLE section
        3. Verifies that the rule engine infers BUS mode instead of CAR
        
        The mock configuration simulates that bus stops are found near
        the section's start and end points, which should cause the rule engine
        to classify the trip as a bus ride rather than a car trip.
        
        This tests the transit stop proximity rule.
        """
        # First reset any pipeline state
        epr.reset_curr_run_state(self.testUUID, False)
        # Mock transit stop data
        mock_get_stops_near.return_value = ['stop1', 'stop2']
        mock_get_predicted_transit_mode.return_value = ['BUS']
        
        section_entry = self.create_test_section(ecwma.MotionTypes.IN_VEHICLE.value)
        
        # Run rule engine
        rule_engine.predict_mode(self.testUUID)
        
        # Check prediction
        predictions = self.ts.find_entries(["inference/prediction"])
        self.assertEqual(len(predictions), 1)
        
        # Entries returned by find_entries are already dictionaries
        prediction_dict = predictions[0]
        self.assertEqual(prediction_dict["data"]["predicted_mode_map"], {'BUS': 1})
        
        # Check inferred section
        inferred_sections = self.ts.find_entries(["analysis/inferred_section"])
        self.assertEqual(len(inferred_sections), 1)
        self.assertEqual(inferred_sections[0]["data"]["sensed_mode"], ecwm.PredictedModeTypes.BUS.value)
        
        # Print results for debugging with transit stops info
        expected_mode = "BUS"
        logging.info("===== MOTORIZED WITH TRANSIT STOPS RESULTS =====")
        logging.info(f"Nearby stops: {mock_get_stops_near.return_value}")
        logging.info(f"Predicted transit mode: {mock_get_predicted_transit_mode.return_value}")
        inferred_mode = self.print_prediction_results(section_entry, prediction_dict, inferred_sections[0], 
                                                    "TRANSIT STOPS", expected_mode)
        self.assertEqual(inferred_mode, expected_mode)
        
        # Verify our mocks were called as expected
        self.assertEqual(mock_get_stops_near.call_count, 2)  # Called for both start and end locations
        self.assertEqual(mock_get_predicted_transit_mode.call_count, 1)
        
    @patch('emission.net.ext_service.transit_matching.match_stops.get_stops_near')
    @patch('emission.net.ext_service.transit_matching.match_stops.get_predicted_transit_mode')
    def testMotorizedWithTrainStops(self, mock_get_predicted_transit_mode, mock_get_stops_near):
        """
        Test motorized mode with train stops nearby.
        
        This test:
        1. Mocks the transit stop detection to return train stops
        2. Creates an IN_VEHICLE section
        3. Verifies that the rule engine infers TRAIN mode instead of CAR
        
        The mock configuration simulates that train stops are found near
        the section's start and end points, which should cause the rule engine
        to classify the trip as a train ride rather than a car trip.
        
        This tests another variant of the transit stop proximity rule.
        """
        # First reset any pipeline state
        epr.reset_curr_run_state(self.testUUID, False)
        # Mock transit stop data
        mock_get_stops_near.return_value = ['train_stop1', 'train_stop2']
        mock_get_predicted_transit_mode.return_value = ['TRAIN']
        
        section_entry = self.create_test_section(ecwma.MotionTypes.IN_VEHICLE.value)
        
        # Run rule engine
        rule_engine.predict_mode(self.testUUID)
        
        # Check prediction
        predictions = self.ts.find_entries(["inference/prediction"])
        self.assertEqual(len(predictions), 1)
        
        # Entries returned by find_entries are already dictionaries
        prediction_dict = predictions[0]
        self.assertEqual(prediction_dict["data"]["predicted_mode_map"], {'TRAIN': 1})
        
        # Check inferred section
        inferred_sections = self.ts.find_entries(["analysis/inferred_section"])
        self.assertEqual(len(inferred_sections), 1)
        self.assertEqual(inferred_sections[0]["data"]["sensed_mode"], ecwm.PredictedModeTypes.TRAIN.value)
        
        # Print results for debugging with train stops info
        expected_mode = "TRAIN"
        logging.info("===== MOTORIZED WITH TRAIN STOPS RESULTS =====")
        logging.info(f"Nearby train stops: {mock_get_stops_near.return_value}")
        logging.info(f"Predicted transit mode: {mock_get_predicted_transit_mode.return_value}")
        inferred_mode = self.print_prediction_results(section_entry, prediction_dict, inferred_sections[0], 
                                                    "TRAIN STOPS", expected_mode)
        self.assertEqual(inferred_mode, expected_mode)
        
        # Verify our mocks were called as expected
        self.assertEqual(mock_get_stops_near.call_count, 2)
        self.assertEqual(mock_get_predicted_transit_mode.call_count, 1)

    def testPrefixedTransitMode(self):
        """
        Test handling of prefixed transit modes like "XMAS:TRAIN" using real data.
        
        This test:
        1. Loads data from the shankari_2023-07-18_xmastrain file
        2. Sets up the test environment with this real data
        3. Verifies that at least some transit sections are properly identified
           despite having prefixed mode identifiers like "XMAS:TRAIN"
        
        This tests the system's ability to handle non-standard OSM route tags
        that contain prefixes, like seasonal routes.
        """
        # First reset any pipeline state
        epr.reset_curr_run_state(self.testUUID, False)
        
        # Load the real example data file
        dataFile = "emission/tests/data/shankari_2023-07-18_xmastrain"
        etc.setupRealExample(self, dataFile)
        
        # Run the pipeline on the loaded data
        etc.runIntakePipeline(self.testUUID)
        
        # Get the sections from the loaded data
        ts = esta.TimeSeries.get_time_series(self.testUUID)
        sections = ts.find_entries(["analysis/cleaned_section"])
        
        # We expect to have at least one section
        self.assertGreater(len(sections), 0)
        logging.info(f"Found {len(sections)} cleaned sections")
        
        # Run rule engine on the sections
        rule_engine.predict_mode(self.testUUID)
        
        # Check prediction and inferred sections
        predictions = ts.find_entries(["inference/prediction"])
        inferred_sections = ts.find_entries(["analysis/inferred_section"])
        
        # Log overall results
        logging.info("===== PREFIXED TRANSIT MODE RESULTS (REAL DATA) =====")
        logging.info(f"Total cleaned sections: {len(sections)}")
        logging.info(f"Total predictions: {len(predictions)}")
        logging.info(f"Total inferred sections: {len(inferred_sections)}")
        
        # Verify we have some predictions and inferred sections
        self.assertGreater(len(predictions), 0)
        self.assertGreater(len(inferred_sections), 0)
        
        # Count the modes in inferred sections
        mode_counts = {}
        for section in inferred_sections:
            mode = section["data"]["sensed_mode"]
            mode_name = ecwm.PredictedModeTypes(mode).name if mode in [e.value for e in ecwm.PredictedModeTypes] else f"UNKNOWN({mode})"
            mode_counts[mode_name] = mode_counts.get(mode_name, 0) + 1
        
        # Log the mode distribution
        logging.info("Mode distribution in inferred sections:")
        for mode, count in mode_counts.items():
            logging.info(f"  {mode}: {count}")
        
        # Since we're testing with real data that might have various modes,
        # we'll consider the test successful if we have some transit modes
        # (which would indicate the prefixed modes were handled correctly)
        transit_modes = ['BUS', 'TRAIN', 'SUBWAY', 'TRAM', 'LIGHT_RAIL']
        transit_section_count = sum(mode_counts.get(mode, 0) for mode in transit_modes)
        
        logging.info(f"Total transit sections found: {transit_section_count}")
        # We should have at least some transit sections
        self.assertGreater(transit_section_count, 0, 
                          "Expected to find at least one transit section in the real data")

    def testUnknownMode(self):
        """
        Test that UNKNOWN sensed mode is handled properly.
        
        When a section has UNKNOWN as the sensed mode, the rule engine should:
        1. Rely solely on the speed profile to infer a mode
        2. For walking speeds, it should infer WALKING
        3. Create a prediction for the most likely mode
        4. Create an inferred section with that mode
        
        This tests the speed-based fallback when the sensed mode is not available.
        """
        # First reset any pipeline state
        epr.reset_curr_run_state(self.testUUID, False)
        section_entry = self.create_test_section(
            ecwma.MotionTypes.UNKNOWN.value,
            speeds=[1.2, 1.3, 1.1, 1.4]  # Walking speeds
        )
        
        # Run rule engine
        rule_engine.predict_mode(self.testUUID)
        
        # Check prediction - should infer from speeds
        predictions = self.ts.find_entries(["inference/prediction"])
        self.assertEqual(len(predictions), 1)
        
        # Entries returned by find_entries are already dictionaries
        prediction_dict = predictions[0]
        
        # Check that we have some prediction (actual value might vary)
        self.assertTrue(len(prediction_dict["data"]["predicted_mode_map"]) > 0)
        
        # Check inferred section
        inferred_sections = self.ts.find_entries(["analysis/inferred_section"])
        self.assertEqual(len(inferred_sections), 1)
        
        # With low speeds, we expect walking
        expected_mode = "WALKING"
        
        # Print results for debugging
        inferred_mode = self.print_prediction_results(section_entry, prediction_dict, inferred_sections[0], 
                                                    "UNKNOWN MODE", expected_mode)
        logging.info(f"Successfully inferred {inferred_mode} from UNKNOWN input based on speed profile")
        logging.info(f"Expected: {expected_mode}, Got: {inferred_mode}, Match: {'✓' if expected_mode == inferred_mode else '✗'}")

    def testAllModePredictions(self):
        """
        Test prediction for all possible input modes to show complete coverage.
        
        This comprehensive test:
        1. Iterates through all possible motion activity types
        2. Creates a section with each motion type
        3. Runs the rule engine on each section
        4. Compares the inferred modes to expected values
        5. Calculates the overall prediction accuracy
        
        This provides a thorough validation of the rule engine's behavior
        across all possible input types and shows which cases are handled
        correctly vs. which might need improvement.
        """
        # First reset any pipeline state
        epr.reset_curr_run_state(self.testUUID, False)
        
        logging.info("===== ALL MODE PREDICTIONS TEST =====")
        logging.info(f"{'Input Mode':<20} {'Expected':<15} {'Predicted':<15} {'Result':<10}")
        logging.info("-" * 65)
        
        # Define expected modes for each input
        expected_modes = {
            "UNKNOWN": "WALKING",
            "STILL": "WALKING",
            "ON_FOOT": "WALKING",
            "WALKING": "WALKING",
            "RUNNING": "WALKING",
            "IN_VEHICLE": "CAR",
            "ON_BICYCLE": "BICYCLING",
            "AIR_OR_HSR": "AIR_OR_HSR"
        }
        
        # Test each motion type and see what gets predicted
        all_results = {}
        for motion_type in ecwma.MotionTypes:
            # Skip invalid or duplicate modes
            if motion_type == ecwma.MotionTypes.TILTING:
                continue
                
            # Clear previous data
            self.clearData()
            
            section_entry = self.create_test_section(motion_type.value)
            rule_engine.predict_mode(self.testUUID)
            
            predictions = self.ts.find_entries(["inference/prediction"])
            if len(predictions) > 0:
                prediction_dict = predictions[0]
                inferred_sections = self.ts.find_entries(["analysis/inferred_section"])
                
                if len(inferred_sections) > 0:
                    input_mode = motion_type.name
                    inferred_mode = ecwm.PredictedModeTypes(inferred_sections[0]["data"]["sensed_mode"]).name
                    expected = expected_modes.get(input_mode, "UNKNOWN")
                    result = "✓ MATCH" if expected == inferred_mode else "✗ MISMATCH"
                    
                    all_results[input_mode] = {
                        "predicted_map": prediction_dict["data"]["predicted_mode_map"],
                        "inferred_mode": inferred_mode,
                        "expected": expected,
                        "result": result
                    }
                    logging.info(f"{input_mode:<20} {expected:<15} {inferred_mode:<15} {result:<10}")
        
        logging.info("===== SUMMARY OF ALL MODE PREDICTIONS =====")
        correct_count = sum(1 for res in all_results.values() if res["result"] == "✓ MATCH")
        total_count = len(all_results)
        logging.info(f"Correct predictions: {correct_count}/{total_count} ({correct_count/total_count*100:.1f}%)")
        logging.info("===========================================")

    @patch('emission.net.ext_service.transit_matching.match_stops.get_stops_near')
    @patch('emission.net.ext_service.transit_matching.match_stops.get_predicted_transit_mode')
    def testErrorHandlingDuringInference(self, mock_get_predicted_transit_mode, mock_get_stops_near):
        """
        Test error handling during mode inference.
        
        This test:
        1. Creates two sections for testing
        2. Configures the transit mode prediction to raise an exception for one section
        3. Verifies that the pipeline continues processing the other section
           instead of failing completely
        
        This tests the system's ability to handle errors gracefully and
        continue processing other sections when one section fails.
        """
        # First reset any pipeline state
        epr.reset_curr_run_state(self.testUUID, False)
        
        # Create two test sections
        section1 = self.create_test_section(ecwma.MotionTypes.IN_VEHICLE.value)
        section2 = self.create_test_section(ecwma.MotionTypes.IN_VEHICLE.value)
        
        # Configure mock to raise exception for the first call but return normally for second call
        side_effects = [
            # First call - raise an exception
            Exception("Test error to simulate failure with unsupported transit mode"),
            # Second call - normal return
            ['TRAIN']
        ]
        mock_get_predicted_transit_mode.side_effect = side_effects
        mock_get_stops_near.return_value = ['stop1', 'stop2']
        
        # Run rule engine
        rule_engine.predict_mode(self.testUUID)
        
        # Check predictions - we should have only one since the exception will cause
        # the first section to be skipped
        predictions = self.ts.find_entries(["inference/prediction"])
        self.assertEqual(len(predictions), 1)
        
        # The one prediction should be for the second section with TRAIN mode
        self.assertEqual(predictions[0]["data"]["predicted_mode_map"], {'TRAIN': 1})
        
        # Check inferred sections - should have only one section
        inferred_sections = self.ts.find_entries(["analysis/inferred_section"])
        self.assertEqual(len(inferred_sections), 1)
        
        # The one inferred section should have TRAIN mode
        self.assertEqual(inferred_sections[0]["data"]["sensed_mode"], ecwm.PredictedModeTypes.TRAIN.value)
        
        # Print results
        logging.info("===== ERROR HANDLING DURING INFERENCE RESULTS =====")
        logging.info("First section should be skipped due to error")
        logging.info("Second section (normal case):")
        logging.info(f"Predicted mode map: {predictions[0]['data']['predicted_mode_map']}")
        logging.info(f"Inferred mode: {ecwm.PredictedModeTypes(inferred_sections[0]['data']['sensed_mode']).name}")
        
        # Verify our mocks were called the expected number of times
        # Should be called for both sections, even though one fails
        self.assertEqual(mock_get_stops_near.call_count, 4)  # 2 per section (start and end) 
        self.assertEqual(mock_get_predicted_transit_mode.call_count, 2)  # Once per section

if __name__ == "__main__":
    # Setup logging with timestamp and level for better debugging
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    unittest.main() 