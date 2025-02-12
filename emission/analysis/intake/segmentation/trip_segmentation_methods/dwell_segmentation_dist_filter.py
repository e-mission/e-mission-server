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


def haversine(lon1, lat1, lon2, lat2):
    """
    Computes the great-circle distance between two arrays of longitude and latitude
    points. Uses the haversine formula.

    :param lon1: array-like longitudes for the first set of points
    :param lat1: array-like latitudes for the first set of points
    :param lon2: array-like longitudes for the second set of points
    :param lat2: array-like latitudes for the second set of points
    :return: array-like distances in meters
    """
    earth_radius = 6371000
    # Convert coordinates to radians for computation.
    lat1, lat2 = np.radians(lat1), np.radians(lat2)
    lon1, lon2 = np.radians(lon1), np.radians(lon2)

    dlat = lat2 - lat1
    dlon = lon2 - lon1
    # Haversine formula for computing great-circle distance.
    a = np.sin(dlat / 2.0) ** 2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon / 2.0) ** 2
    return 2 * earth_radius * np.arcsin(np.sqrt(a))


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
        :param point_threshold: The number of points to look back for a valid trip segmentation.
        :param distance_threshold: The minimum distance (in meters) between points for a valid segmentation.
        """
        self.time_threshold = time_threshold
        self.point_threshold = point_threshold
        self.distance_threshold = distance_threshold

    def segment_into_trips(self, timeseries, time_query, filtered_points_df):
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
        
        A subsequent row-by-row check is performed for candidate points to invalidate
        any with a huge invalid timestamp offset.
        
        Finally, the candidate points are used to split the filtered points into trips.
        
        :param timeseries: The timeseries database interface
        :param time_query: The time range query to select relevant entries.
        :param filtered_points_df: A pandas DataFrame containing filtered location points.
        :return: A list of tuples. Each tuple contains a start and end point (wrapped as AttrDict)
                 that demarcate a trip.
        """
        # Start timer to record time spent retrieving and preparing the filtered points.
        with ect.Timer() as t_get_filtered_points:
            # Make a copy and reset the index so that vectorized operations (e.g. .diff()) work as expected.
            df = filtered_points_df.copy().reset_index(drop=True)
            user_id = df["user_id"].iloc[0]
            # Initially, assume that all points are valid.
            df['valid'] = True
        esds.store_pipeline_time(
            user_id,
            ecwp.PipelineStages.TRIP_SEGMENTATION.name + "/segment_into_trips_dist/get_filtered_points_df",
            time.time(),
            t_get_filtered_points.elapsed
        )

        # Retrieve transition events (e.g. tracking restarts) for the given time range.
        self.transition_df = timeseries.get_data_df("statemachine/transition", time_query)
        # Retrieve motion events as a DataFrame for vectorized operations.
        motion_df = timeseries.get_data_df("background/motion_activity", time_query)
        # Also obtain a list of raw motion events; this is used for a more detailed row-by-row check.
        motion_list = list(timeseries.find_entries(["background/motion_activity"], time_query))

        # --- Compute vectorized differences ---
        # Calculate the time difference (in seconds) between consecutive points.
        df['delta_ts'] = df['ts'].diff()
        # Calculate the distance difference between consecutive points using the haversine function.
        df['delta_dist'] = haversine(
            df['longitude'].shift(1).to_numpy(), df['latitude'].shift(1).to_numpy(),
            df['longitude'].to_numpy(), df['latitude'].to_numpy()
        )
        # Compute speed (meters per second) from the distance and time differences.
        df['speed'] = df['delta_dist'] / df['delta_ts']

        # --- Compute auxiliary flags ---
        # Flag points where the tracking has been restarted (e.g., a new sensor session).
        df['tracking_restarted'] = eaisr.tracking_restarted_in_loc_df(df, self.transition_df)
        # Flag points where there is ongoing motion based on sensor-detected motion activities.
        df['ongoing_motion'] = eaisr.ongoing_motion_in_loc_df(df, motion_df)

        # --- Define thresholds for segmentation ---
        # For a candidate segmentation, if the speed is below this threshold, then the phone may be stationary.
        speedThreshold = old_div(float(self.distance_threshold * 2), (old_div(self.time_threshold, 2)))
        # Define an upper bound: if the time gap exceeds 12 hours, force a segmentation.
        TWELVE_HOURS = 12 * 60 * 60

        # --- Identify candidate segmentation points ---
        # A candidate segmentation point must have:
        #   (i) A time gap greater than time_threshold, AND
        #  (ii) Either a tracking restart occurred, there is no ongoing motion,
        #       the time gap exceeds 12 hours, or the computed speed is below speedThreshold,
        #   (iii) The distance gap is at least the distance_threshold.
        candidate_flag = (df['delta_ts'] > self.time_threshold) & (
            (df['tracking_restarted']) |
            (~df['ongoing_motion']) |
            (df['delta_ts'] > TWELVE_HOURS) |
            (df['speed'] < speedThreshold)
        ) & (df['delta_dist'] >= self.distance_threshold)
        # Replace any missing values (NaN) with False.
        candidate_flag = candidate_flag.fillna(False)

        # --- Row-by-row check for huge invalid timestamp offset ---
        # Even after the vectorized check, we need to iterate over candidate points to handle edge cases
        # where an abnormal gap might be due to an invalid timestamp.
        huge_invalid = np.zeros(len(df), dtype=bool)
        # Define a candidate condition: time gap too high and speed too low.
        candidate_condition = (df['delta_ts'] > self.time_threshold) & (df['speed'] < speedThreshold)
        # Loop over indices that meet the candidate condition.
        for i in np.where(candidate_condition)[0]:
            if i == 0:
                # Cannot check for a huge offset on the very first point.
                continue
            lastPoint = ad.AttrDict(df.iloc[i - 1])
            currPoint = ad.AttrDict(df.iloc[i])
            # Get raw motion events in the time range between lastPoint and currPoint.
            ongoing_motion_range = eaisr.get_ongoing_motion_in_range(
                lastPoint.ts, currPoint.ts, timeseries, motion_list
            )
            # If a huge invalid timestamp offset is detected, mark the point as invalid.
            if eaistc.is_huge_invalid_ts_offset(self, lastPoint, currPoint, timeseries, ongoing_motion_range):
                huge_invalid[i] = True
                df.at[i, 'valid'] = False
                # Invalidate the raw data entry in the timeseries database.
                timeseries.invalidate_raw_entry(currPoint["_id"])
        # Exclude any candidates that were invalidated due to huge timestamp offsets.
        candidate_flag = candidate_flag & (~huge_invalid)

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
                # Start a new trip at the candidate index.
                trip_start_idx = idx
        # If there are remaining points after the last candidate, add them as the final trip.
        if trip_start_idx < len(df):
            segmentation_idx_pairs.append((trip_start_idx, len(df) - 1))

        # --- Force trip end at the final point if a transition event occurs ---
        # If there is evidence (from transition events) that the user has stopped moving
        # after the last point in our data, force the end of the trip at the final point.
        if len(self.transition_df) > 0:
            last_point = ad.AttrDict(df.iloc[-1])
            stopped_moving_after_last = self.transition_df[
                (self.transition_df.ts > last_point.ts) & (self.transition_df.transition == 2)
            ]
            if len(stopped_moving_after_last) > 0:
                if segmentation_idx_pairs:
                    # Extend the last trip segment to the very last point.
                    segmentation_idx_pairs[-1] = (segmentation_idx_pairs[-1][0], len(df) - 1)
                else:
                    # If no segmentation has been found so far, consider the entire series as one trip.
                    segmentation_idx_pairs.append((0, len(df) - 1))
                # Record the last processed timestamp.
                self.last_ts_processed = float(df.iloc[-1]['metadata_write_ts'])

        # Record the time spent in the segmentation loop.
        esds.store_pipeline_time(
            user_id,
            ecwp.PipelineStages.TRIP_SEGMENTATION.name + "/segment_into_trips_dist/loop",
            time.time(),
            t_get_filtered_points.elapsed
        )

        # --- Convert index pairs to segmentation points ---
        # For each pair of start and end indices, wrap the corresponding rows in an AttrDict
        # so that downstream processes can access fields via attributes.
        segmentation_points = [
            (ad.AttrDict(df.iloc[start_idx]), ad.AttrDict(df.iloc[end_idx]))
            for (start_idx, end_idx) in segmentation_idx_pairs
        ]
        return segmentation_points
