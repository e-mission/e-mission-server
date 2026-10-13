import unittest
import pandas as pd
import numpy as np
import logging
import datetime

import emission.core.wrapper.transition as ecwt
import emission.core.wrapper.motionactivity as ecwm
import emission.analysis.intake.segmentation.restart_checking as restart_checking

import emission.storage.timeseries.abstract_timeseries as esta
import emission.storage.timeseries.timequery as estt
import emission.core.get_database as edb

# Test imports
import emission.tests.common as etc

class TestRestartChecking(unittest.TestCase):
    
    def setUp(self):
        logging.basicConfig(level=logging.DEBUG)
    
    def test_ongoing_motion_in_loc_df(self):
        # dummy data
        loc_df = pd.DataFrame({
            'ts': [100, 200, 300, 400, 500],
            'latitude': [37.1, 37.2, 37.3, 37.4, 37.5],
            'longitude': [-122.1, -122.2, -122.3, -122.4, -122.5]
        })
        
        # Test case 1: Empty motion dataframe
        motion_df_empty = pd.DataFrame(columns=['ts', 'type'])
        result_empty = restart_checking.ongoing_motion_in_loc_df(loc_df, motion_df_empty)
        self.assertEqual(len(result_empty), len(loc_df))
        self.assertTrue(all(result_empty == False))
        
        # Test case 2: Motion timestamps between location points
        motion_df = pd.DataFrame({
            'ts': [150, 350, 600],
            'type': [1, 2, 3]
        })
        
        result = restart_checking.ongoing_motion_in_loc_df(loc_df, motion_df)
        self.assertEqual(len(result), len(loc_df))
        
        # Expected: 150 maps to index 1 (200), 350 maps to index 3 (400), 600 is beyond the range
        expected = pd.Series([False, True, False, True, False], index=loc_df.index)
        pd.testing.assert_series_equal(result, expected)
        
        # Test case 3: All motion timestamps after location timestamps
        motion_df_after = pd.DataFrame({
            'ts': [600, 700, 800],
            'type': [1, 2, 3]
        })
        result_after = restart_checking.ongoing_motion_in_loc_df(loc_df, motion_df_after)
        self.assertTrue(all(result_after == False))
        
        # Test case 4: All motion timestamps before location points
        motion_df_before = pd.DataFrame({
            'ts': [10, 20, 30],
            'type': [1, 2, 3]
        })
        result_before = restart_checking.ongoing_motion_in_loc_df(loc_df, motion_df_before)
        # Important note: searchsorted returns index 0 for values before the first element
        # This means values 10, 20, 30 all map to index 0 (100)
        expected_before = pd.Series([True, False, False, False, False], index=loc_df.index)
        pd.testing.assert_series_equal(result_before, expected_before)
    
    def test_tracking_restarted_in_loc_df(self):
        # more dummy data
        loc_df = pd.DataFrame({
            'ts': [100, 200, 300, 400, 500],
            'latitude': [37.1, 37.2, 37.3, 37.4, 37.5],
            'longitude': [-122.1, -122.2, -122.3, -122.4, -122.5]
        })
        
        # Test case 1: Empty transition dataframe
        transition_df_empty = pd.DataFrame(columns=['ts', 'transition', 'curr_state'])
        result_empty = restart_checking.tracking_restarted_in_loc_df(loc_df, transition_df_empty)
        self.assertEqual(len(result_empty), len(loc_df))
        self.assertTrue(all(result_empty == False))
        
        # Test case 2: Transitions between location points
        transition_df = pd.DataFrame({
            'ts': [150, 250, 450, 600],
            'transition': [
                ecwt.TransitionType.STOP_TRACKING.value,
                ecwt.TransitionType.BOOTED.value,
                ecwt.TransitionType.VISIT_ENDED.value,
                ecwt.TransitionType.BOOTED.value
            ],
            'curr_state': [
                ecwt.State.WAITING_FOR_TRIP_START.value,
                ecwt.State.WAITING_FOR_TRIP_START.value,
                ecwt.State.WAITING_FOR_TRIP_START.value,
                ecwt.State.WAITING_FOR_TRIP_START.value
            ]
        })
        
        result = restart_checking.tracking_restarted_in_loc_df(loc_df, transition_df)
        self.assertEqual(len(result), len(loc_df))
        
        # Expected: 150 maps to index 1 (200), 250 maps to index 2 (300), 
        # 450 maps to index 4 (500), 600 is beyond the range
        expected = pd.Series([False, True, True, False, True], index=loc_df.index)
        pd.testing.assert_series_equal(result, expected)
        
        # Test case 3: Non-restart transitions
        transition_df_non_restart = pd.DataFrame({
            'ts': [150, 250, 450],
            'transition': [
                ecwt.TransitionType.STOPPED_MOVING.value,
                ecwt.TransitionType.EXITED_GEOFENCE.value,
                ecwt.TransitionType.VISIT_STARTED.value
            ],
            'curr_state': [
                ecwt.State.ONGOING_TRIP.value,
                ecwt.State.ONGOING_TRIP.value,
                ecwt.State.WAITING_FOR_TRIP_START.value
            ]
        })
        
        result_non_restart = restart_checking.tracking_restarted_in_loc_df(loc_df, transition_df_non_restart)
        self.assertTrue(all(result_non_restart == False))

    def test_real_reboot_example_pipeline(self):
        """Test with real example data that includes reboots and transitions"""
        # Setup real example data with a dataset that has transitions due to reboots and untracked time
        etc.setupRealExample(self, "emission/tests/data/real_examples/shankari_untracked_time_jan_15_reboot_multi_day")
        
        ts = esta.TimeSeries.get_time_series(self.testUUID)
        
        # First scan the data to find time ranges with transitions
        # Get a broader range to increase chances of finding transitions
        scan_query = estt.TimeQuery("metadata.write_ts", 0, float('inf'))
        transition_check = ts.get_data_df('statemachine/transition', scan_query)
        
        if len(transition_check) == 0:
            logging.warning("No transitions found in the dataset, skipping this test")
            self.skipTest("No transitions found in the reboot dataset")
            return
        
        # Find time ranges with transitions
        logging.debug(f"Found {len(transition_check)} transitions in the dataset")
        
        # Get the range containing transitions
        min_ts = transition_check['ts'].min()
        max_ts = transition_check['ts'].max()
        padding = 3600  # 1 hour padding -- can be changed to more or less
        time_query = estt.TimeQuery("metadata.write_ts", min_ts - padding, max_ts + padding)
        
        # Get the data from the time range with transitions
        loc_df = ts.get_data_df('background/filtered_location', time_query)
        motion_df = ts.get_data_df('background/motion_activity', time_query)
        transition_df = ts.get_data_df('statemachine/transition', time_query)
        
        # Log some information about the data
        logging.debug(f"Found {len(loc_df)} location points in time range")
        logging.debug(f"Found {len(motion_df)} motion activities in time range")
        logging.debug(f"Found {len(transition_df)} transitions in time range")
        
        if len(loc_df) == 0 or len(transition_df) == 0:
            logging.warning("Insufficient data found in the time range with transitions")
            self.skipTest("Insufficient data in the time range with transitions")
            return
        
        if 'transition' in transition_df.columns and 'curr_state' in transition_df.columns:
            # Test ongoing_motion_in_loc_df if motion data exists
            if len(motion_df) > 0:
                ongoing_motion = restart_checking.ongoing_motion_in_loc_df(loc_df, motion_df)
                logging.debug(f"ongoing_motion true count: {sum(ongoing_motion)}")
                self.assertEqual(len(ongoing_motion), len(loc_df))
            
            # Test tracking_restarted_in_loc_df
            tracking_restarted = restart_checking.tracking_restarted_in_loc_df(loc_df, transition_df)
            logging.debug(f"tracking_restarted true count: {sum(tracking_restarted)}")
            self.assertEqual(len(tracking_restarted), len(loc_df))
            
            # Verify the restart tracking logic directly
            restart_transitions_df = transition_df[
                (transition_df.transition == ecwt.TransitionType.STOP_TRACKING.value) |
                (transition_df.transition == ecwt.TransitionType.BOOTED.value) |
                ((transition_df.curr_state == ecwt.State.WAITING_FOR_TRIP_START.value) &
                 (transition_df.transition == ecwt.TransitionType.VISIT_ENDED.value))
            ]
            
            if len(restart_transitions_df) > 0:
                logging.debug(f"Found {len(restart_transitions_df)} restart transitions")
                restart_timestamps = restart_transitions_df['ts'].values
                restart_idxs = np.searchsorted(loc_df['ts'].values, restart_timestamps)
                restart_idxs = restart_idxs[restart_idxs < len(loc_df)]
                logging.debug(f"restart_timestamps = {restart_timestamps[:5]}")
                logging.debug(f"restart_idxs = {restart_idxs[:5]}")
                
                expected_restart = pd.Series(False, index=loc_df.index)
                expected_restart[restart_idxs] = True
                pd.testing.assert_series_equal(tracking_restarted, expected_restart)
                
                # searchsorted maps timestamps to location indices
                if len(restart_idxs) > 0:
                    logging.debug("Demonstrating searchsorted mapping:")
                    for idx, ts in enumerate(restart_timestamps[:min(5, len(restart_timestamps))]):
                        if idx < len(restart_idxs):
                            loc_idx = restart_idxs[idx]
                            if loc_idx < len(loc_df):
                                loc_ts = loc_df['ts'].iloc[loc_idx]
                                logging.debug(f"  Restart timestamp {ts} maps to location index {loc_idx} with ts {loc_ts}")
                                logging.debug(f"  The restart transition is: {restart_transitions_df.iloc[idx]['transition']}")
            else:
                logging.warning("No restart transitions found in the transitions data")
        else:
            logging.warning("Transition data doesn't have the expected columns structure")
    
    def tearDown(self):
        if hasattr(self, 'testUUID'):
            logging.debug("Cleaning up database entries for %s" % self.testUUID)
            edb.get_timeseries_db().delete_many({"user_id": self.testUUID})
            edb.get_analysis_timeseries_db().delete_many({"user_id": self.testUUID})

if __name__ == '__main__':
    import emission.tests.common as etc
    etc.configLogging()
    unittest.main() 