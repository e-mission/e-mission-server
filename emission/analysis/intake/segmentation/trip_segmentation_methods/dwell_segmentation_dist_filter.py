from __future__ import division, unicode_literals, print_function, absolute_import
from future import standard_library
standard_library.install_aliases()
from builtins import str, range, zip
from past.utils import old_div
import logging
import attrdict as ad
import numpy as np
import pandas as pd
import time

# Our imports
import emission.analysis.point_features as pf
import emission.analysis.intake.segmentation.trip_segmentation as eaist
import emission.core.wrapper.location as ecwl

import emission.analysis.intake.segmentation.restart_checking as eaisr
import emission.analysis.intake.segmentation.trip_segmentation_methods.trip_end_detection_corner_cases as eaistc
import emission.storage.decorations.stats_queries as esds
import emission.core.timer as ect
import emission.core.wrapper.pipelinestate as ecwp
from emission.core.common import haversine_numpy

TWELVE_HOURS = 12 * 60 * 60


class DwellSegmentationDistFilter(eaist.TripSegmentationMethod):
    def __init__(self, time_threshold, point_threshold, distance_threshold):
        """
        Determines segmentation points for location points that were generated using a
        distance filter (i.e. the phone reports a new point every n meters). Note that
        this algorithm expects a time gap between subsequent points (even when generated
        via a distance filter) to detect a trip end. For example, on iOS, a few extra points
        may be reported even when the phone is not in motion (possibly because of low-quality
        GPS fixes causing zigzagging). These extra points need to be filtered out.

        :param time_threshold: The minimum time gap (in seconds) between points to consider a trip end.
        :param point_threshold: (Currently unused) The number of points to look back for a valid trip segmentation.
        :param distance_threshold: The minimum distance (in meters) between points for a valid segmentation.
        """
        self.time_threshold = time_threshold
        self.point_threshold = point_threshold
        self.distance_threshold = distance_threshold

    def segment_into_trips(self, loc_df, transition_df, motion_df):
        """
        A vectorized implementation of the distance–based segmentation.
        This function examines a range of the timeseries data and returns a list of
        segmentation points (as pairs of start and end points) that define trips.
        
        The algorithm computes time differences, distance differences (using the haversine function)
        and speeds between successive points. In addition, it computes two flags:
          - 'tracking_restarted': whether the tracking has been restarted (e.g. due to app restart)
          - 'ongoing_motion': whether there is evidence of motion activity between points.
        
        Candidate segmentation points are those where:
          - The time gap exceeds the time_threshold, AND
          - One or more of the following conditions hold:
              * Tracking has restarted,
              * There is no ongoing motion,
              * The time gap is extremely large (e.g. > 12 hours), or
              * The computed speed is below a threshold (indicating little movement)
          - AND the distance gap is at least the distance_threshold.
        
        A placeholder for a row-by-row check is included for huge invalid timestamp offsets,
        although currently no individual point is invalidated.
        
        Finally, the candidate points are used to split the filtered points into trips.
        
        :param timeseries: The timeseries database interface
        :param time_query: The time range query to select relevant entries.
        :param loc_df: A pandas DataFrame containing filtered location points.
        :return: A list of tuples. Each tuple contains a start and end point (wrapped as AttrDict)
                 that demarcate a trip.
        """
        user_id = loc_df["user_id"].iloc[0]
        loc_df['valid'] = True
        loc_df['index'] = loc_df.index

        self.transition_df = transition_df
        self.motion_df = motion_df

        # --- Compute vectorized differences ---
        # Calculate the time difference (in seconds) between consecutive points.
        loc_df['ts_diff'] = loc_df['ts'].diff()
        # Calculate the distance difference between consecutive points using the haversine function.
        loc_df['dist_diff'] = haversine_numpy(
            loc_df['longitude'].shift(1).to_numpy(), loc_df['latitude'].shift(1).to_numpy(),
            loc_df['longitude'].to_numpy(), loc_df['latitude'].to_numpy()
        )
        # Compute speed (meters per second) from the distance and time differences.
        loc_df['speed'] = loc_df['dist_diff'] / loc_df['ts_diff']

        # --- Compute auxiliary flags ---
        # Flag points where the tracking has been restarted (e.g., a new sensor session).
        loc_df['tracking_restarted'] = eaisr.tracking_restarted_in_loc_df(loc_df, self.transition_df)
        # Flag points where there is ongoing motion based on sensor-detected motion activities.
        loc_df['ongoing_motion'] = eaisr.ongoing_motion_in_loc_df(loc_df, self.motion_df)

        # --- Define thresholds for segmentation ---
        # For a candidate segmentation, if the speed is below this threshold, then the phone may be stationary.
        speed_threshold = float(self.distance_threshold * 2) / (self.time_threshold / 2)

        with ect.Timer() as t_loop:
            # --- Identify candidate segmentation points ---
            # A candidate segmentation point must have:
            #   (i) A time gap greater than time_threshold, AND
            #  (ii) Either a tracking restart occurred, there is no ongoing motion,
            #       the time gap exceeds 12 hours, or the computed speed is below speedThreshold,
            #   (iii) The distance gap is at least the distance_threshold.
            candidate_flag = (loc_df['ts_diff'] > self.time_threshold) & (
                (loc_df['tracking_restarted'])
                | (~loc_df['ongoing_motion'])
                | (loc_df['ts_diff'] > TWELVE_HOURS)
                | (loc_df['speed'] < speed_threshold)
            )
            # Replace any missing values (NaN) with False.
            candidate_flag = candidate_flag.fillna(False)

            # --- Placeholder for row-by-row check for huge invalid timestamp offset ---
            # Currently, no individual timestamp is flagged as invalid.
            huge_invalid = np.zeros(len(loc_df), dtype=bool)

            # Exclude any candidates that were invalidated due to huge timestamp offsets.
            candidate_flag = candidate_flag & (~huge_invalid)
            self.last_ts_processed = None

            # --- Split the data into trips using candidate segmentation flags ---
            segmentation_idx_pairs = []
            trip_start_idx = 0
            candidate_indices = np.where(candidate_flag)[0]
            # Iterate over all candidate segmentation points.
            for idx in candidate_indices:
                if idx > trip_start_idx:
                    # Define the current trip to run from trip_start_idx to the point before the candidate index.
                    trip_end_idx = idx - 1
                    segmentation_idx_pairs.append((trip_start_idx, trip_end_idx))
                    self.last_ts_processed = loc_df.iloc[idx]['metadata_write_ts']
                    trip_start_candidates = loc_df[
                        (loc_df['index'] > trip_end_idx) & (loc_df['dist_diff'] >= self.distance_threshold)
                    ]
                    if len(trip_start_candidates) == 0:
                        logging.debug(f'no more candidates after {trip_end_idx}, this is the last trip')
                        trip_start_idx = len(loc_df)
                    else:
                        trip_start_idx = trip_start_candidates.iloc[0]['index']
                        logging.debug(f'using {trip_start_idx} as trip start index')

            # --- Force trip end at the final point if a transition event occurs ---
            # If there is evidence (from transition events) that the user has stopped moving
            # after the last point in our data, force the end of the trip at the final point.
            if len(self.transition_df) > 0 and trip_start_idx < len(loc_df):
                last_point = ad.AttrDict(loc_df.iloc[-1])
                stopped_moving_after_last = self.transition_df[
                    (self.transition_df.ts > last_point.ts) & (self.transition_df.transition == 2)
                ]
                if len(stopped_moving_after_last) > 0:
                    if segmentation_idx_pairs:
                        # Finish out this trip ending at the last point.
                        segmentation_idx_pairs.append((trip_start_idx, len(loc_df) - 1))
                    else:
                        # If no segmentation has been found so far, consider the entire series as one trip.
                        segmentation_idx_pairs.append((0, len(loc_df) - 1))
                    # Record the last processed timestamp.
                    self.last_ts_processed = float(loc_df.iloc[-1]['metadata_write_ts'])

        esds.store_pipeline_time(
            user_id,
            ecwp.PipelineStages.TRIP_SEGMENTATION.name + "/segment_into_trips_dist/loop",
            time.time(),
            t_loop.elapsed
        )

        # --- Convert index pairs to segmentation points ---
        # For each pair of start and end indices, wrap the corresponding rows in an AttrDict
        # so that downstream processes can access fields via attributes.
        segmentation_points = [
            (ad.AttrDict(loc_df.iloc[start_idx]), ad.AttrDict(loc_df.iloc[end_idx]))
            for (start_idx, end_idx) in segmentation_idx_pairs
        ]
        
        logging.info(f'self.last_ts_processed = {self.last_ts_processed}')
        for (p1, p2) in segmentation_points:
            logging.info(f"{p1['index']}, {p1['ts']} -> {p2['index']}, {p2['ts']}")
        return segmentation_points
