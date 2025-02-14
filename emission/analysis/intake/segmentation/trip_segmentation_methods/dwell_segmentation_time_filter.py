from __future__ import division
from __future__ import unicode_literals
from __future__ import print_function
from __future__ import absolute_import
# Standard imports
from future import standard_library
standard_library.install_aliases()
from builtins import str
from builtins import *
from past.utils import old_div
import logging
import attrdict as ad
import numpy as np
import pandas as pd
import datetime as pydt
import time

# Our imports
import emission.analysis.point_features as pf
import emission.analysis.intake.segmentation.trip_segmentation as eaist
import emission.core.wrapper.location as ecwl

import emission.analysis.intake.segmentation.restart_checking as eaisr
import emission.storage.decorations.stats_queries as esds
import emission.core.timer as ect
import emission.core.wrapper.pipelinestate as ecwp
from emission.core.common import haversine_numpy


TWELVE_HOURS = 12 * 60 * 60

class DwellSegmentationTimeFilter(eaist.TripSegmentationMethod):
    def __init__(self, time_threshold, point_threshold, distance_threshold):
        """
        Determines segmentation points for points that were generated using a
        time filter (i.e. report points every n seconds). This will *not* work for
        points generated using a distance filter because it expects to have a
        cluster of points to detect the trip end, and with a distance filter,
        we will not get updates while we are still.

        At least on android, we can get updates at a different frequency than
        the "n" specified above. In particular:
        a) we can get updates more frequently than "n" if there are other apps
            that are requesting updates frequently - for example, while using a routing app.
        b) we can get updates less frequently than "n" if there are bad/low
            accuracy points that are filtered out.

        So we use a combination of a time filter and a "number of points"
            filter to detect the trip end.

        The time_threshold indicates the number of seconds that we need to be
            still before a trip end is detected.
        The point_threshold indicates the number of prior points (after
            filtering) that we need to be still for before a trip end is detected
        The distance_threshold indicates the radius of the circle used to
            detect that we are still. If all the points within the
            time_threshold AND all the points within the point_threshold are
            within the distance_threshold of each other, then we are still.
        """
        self.time_threshold = time_threshold
        self.point_threshold = point_threshold
        self.distance_threshold = distance_threshold


    def segment_into_trips(self, loc_df, transition_df, motion_df):
        """
        Examines the timeseries database for a specific range and returns the
        segmentation points. Note that the input is the entire timeseries and
        the time range. This allows algorithms to use whatever combination of
        data that they want from the sensor streams in order to determine the
        segmentation points.
        """
        user_id = loc_df["user_id"].iloc[0]
        loc_df = loc_df[
            (loc_df.metadata_write_ts - loc_df.ts) < 1000
        ]
        loc_df.reset_index(inplace=True)

        self.transition_df = transition_df
        self.motion_df = motion_df

        if len(self.transition_df) > 0:
            logging.debug(f"self.transition_df = {self.transition_df[['fmt_time', 'transition']]}")
        else:
            logging.debug("self.transition_df is empty")

        self.last_ts_processed = None
        logging.info(f"Last ts processed = {self.last_ts_processed}")

        loc_df['recent_points_diffs'] = self.compute_recent_point_diffs(loc_df)
        loc_df['dist_diff'] = loc_df['recent_points_diffs'].apply(lambda x: x[0, -1])
        loc_df['ts_diff'] = loc_df['recent_points_diffs'].apply(lambda x: x[1, -1])
        loc_df['speed_diff'] = loc_df['dist_diff'] / loc_df['ts_diff']
        loc_df['ongoing_motion'] = eaisr.ongoing_motion_in_loc_df(loc_df, self.motion_df)
        loc_df['tracking_restarted'] = eaisr.tracking_restarted_in_loc_df(loc_df, self.transition_df)
        
        segmentation_idx_pairs = []
        trip_start_idx = 0
        with ect.Timer() as t_loop:
            while trip_start_idx < len(loc_df):
                logging.info(f"trip_start_idx = {trip_start_idx}")
                # trim off dists of points that were before the trip_start_idx
                recent_diffs_filtered = [
                    diffs if row_idx - trip_start_idx > diffs.shape[1]
                    else diffs[:, trip_start_idx - row_idx:]
                    for row_idx, diffs in loc_df['recent_points_diffs'].items()
                ]

                max_recent_dist_diffs = np.array(
                    [np.inf
                        # 'inf' means we can't make a decision because:
                        # we don't have enough recent points (TODO the -2 weird, but necessary to match the current behavior)
                        if diffs.shape[1] < self.point_threshold - 2
                        # or the oldest point within the time filter is not old enough
                        or np.sum(
                            (diffs[1, :] < self.time_threshold) &
                            (diffs[1, :] > self.time_threshold - 30)
                        ) == 0
                     else diffs[0, :].max()
                     for diffs in recent_diffs_filtered]
                )

                potential_trip_end_idxs = np.where(
                    # check points that haven't already been segmented
                    (loc_df.index > trip_start_idx)
                    & (
                        # i) there was a statemachine/transition indicating a restart before this point
                        (loc_df['tracking_restarted'])
                        # ii) big gap and no motion activity since last point
                        | ((loc_df['ts_diff'] > 2 * self.time_threshold)
                           & (~loc_df['ongoing_motion']))
                        # iii) huge gap
                        | ((loc_df['ts_diff'] > TWELVE_HOURS))
                        # iv) big gap and speed < dist_threshold / time_threshold
                        | ((loc_df['ts_diff'] > 2 * self.time_threshold)
                           & (loc_df['speed_diff'] < (self.distance_threshold / self.time_threshold)))
                        # v) common case: sufficient recent points and all are within distance_threshold
                        | (max_recent_dist_diffs < self.distance_threshold)
                    )
                )[0]

                logging.info(f'potential_trip_end_idxs = {potential_trip_end_idxs}')

                if len(potential_trip_end_idxs) == 0:
                    logging.info(f'No more segments, trip_start_idx = {trip_start_idx} / {len(loc_df)-1}')

                    if trip_start_idx < len(loc_df) -1 and len(self.transition_df) > 0:
                        last_point_ts = loc_df.iloc[-1]['ts']
                        stopped_moving_after_last = self.transition_df[
                            (self.transition_df['ts'] > last_point_ts) & (self.transition_df['transition'] == 2)
                        ]
                        logging.info(f"looking after {last_point_ts}, " +
                              f"found transitions {stopped_moving_after_last}")
                        if len(stopped_moving_after_last) > 0:
                            (_, trip_end_idx) = self.get_last_trip_end_point_idx(
                                len(loc_df) - 1,
                                recent_diffs_filtered[len(loc_df) - 1],
                            )
                            segmentation_idx_pairs.append((trip_start_idx, trip_end_idx))
                            logging.info(f'Found trip end at {trip_end_idx}')

                            self.last_ts_processed = float(loc_df.iloc[-1]['metadata_write_ts'])
                    trip_start_idx = len(loc_df)
                    break

                trip_end_detected_idx = potential_trip_end_idxs[0]
                logging.info(f'***** TRIP END DETECTED AT {trip_end_detected_idx} / {len(loc_df)-1} *****')
                logging.info(f'recent_diffs_filtered[{trip_end_detected_idx}] = {recent_diffs_filtered[trip_end_detected_idx]}')
                logging.info(f'max_recent_dist_diffs[{trip_end_detected_idx}] = {max_recent_dist_diffs[trip_end_detected_idx]}')

                ended_before_this, trip_end_idx = self.get_last_trip_end_point_idx(
                    trip_end_detected_idx,
                    recent_diffs_filtered[trip_end_detected_idx],
                )
                segmentation_idx_pairs.append((trip_start_idx, trip_end_idx))

                if ended_before_this:
                    # there was a big gap before trip_end_detected_idx,
                    # which means it is actually the start of the next trip
                    trip_start_idx = trip_end_detected_idx
                    self.last_ts_processed = float(loc_df.iloc[trip_start_idx]['metadata_write_ts'])
                    logging.info(f'set last_ts_processed to {self.last_ts_processed}')
                else:
                    # look for the next point that is outside the filter
                    next_start_idx = loc_df[
                        (loc_df.index > trip_end_detected_idx) & (
                            (loc_df['ts_diff'] > 60)
                            | (loc_df['dist_diff'] >= self.distance_threshold)
                        )
                    ].index

                    if len(next_start_idx) > 0:
                        trip_start_idx = next_start_idx[0]
                        logging.info(f'setting last_ts_processed to {trip_start_idx-1} {loc_df.iloc[trip_start_idx-1]["metadata_write_ts"]}')
                        self.last_ts_processed = float(loc_df.iloc[trip_start_idx-1]['metadata_write_ts'])
                    elif trip_end_detected_idx + 1 < len(loc_df):
                        trip_start_idx = trip_end_detected_idx + 1
                        logging.info(f'setting last_ts_processed to {trip_start_idx} {loc_df.iloc[trip_start_idx]["metadata_write_ts"]}')
                        self.last_ts_processed = float(loc_df.iloc[trip_start_idx]['metadata_write_ts'])
                    else:
                        trip_start_idx = len(loc_df)
                        logging.info(f'setting last_ts_processed to {len(loc_df) - 1} {loc_df.iloc[-1]["metadata_write_ts"]}')
                        self.last_ts_processed = float(loc_df.iloc[-1]['metadata_write_ts'])

        esds.store_pipeline_time(
            user_id,
            ecwp.PipelineStages.TRIP_SEGMENTATION.name + "/segment_into_trips_time/loop",
            time.time(),
            t_loop.elapsed
        )

        segmentation_points = [
            (ad.AttrDict(loc_df.iloc[start_idx]),
             ad.AttrDict(loc_df.iloc[end_idx]))
            for (start_idx, end_idx) in segmentation_idx_pairs
        ]

        logging.info(f'self.last_ts_processed = {self.last_ts_processed}')
        for (p1, p2) in segmentation_points:
            logging.info(f"{p1['index']}, {p1['ts']} -> {p2['index']}, {p2['ts']}")        
        return segmentation_points


    def compute_recent_point_diffs(self, loc_df):
        indices = loc_df.index
        last_10_start_indices = np.searchsorted(indices, indices - self.point_threshold)
        timestamps = loc_df["ts"].to_numpy()
        last_5min_start_indices = np.searchsorted(timestamps, timestamps - self.time_threshold, side='right')

        start_indices = [min(a, b) for a, b in zip(last_10_start_indices, last_5min_start_indices)]
        lat, lon = loc_df["latitude"].to_numpy(), loc_df["longitude"].to_numpy()

        recent_dists_and_times = pd.Series([
            np.array([
                haversine_numpy(
                    lon[start_indices[i]:i], lat[start_indices[i]:i],
                    lon[i], lat[i]
                ),
                timestamps[i] - timestamps[start_indices[i]:i],
            ])
            if start_indices[i] < i else np.empty((2, self.point_threshold))
            for i in range(len(loc_df))
        ])

        return recent_dists_and_times


    def get_last_trip_end_point_idx(self, curr_idx, recent_diffs: np.ndarray):
        recent_diffs_non_na = recent_diffs[:, ~np.isnan(recent_diffs[0, :])]
        num_recent_diffs_in_point_threshold = min(len(recent_diffs_non_na[0, :]), self.point_threshold)
        num_recent_diffs_in_time_threshold = sum(recent_diffs_non_na[1, :] < self.time_threshold)
        ended_before_this = num_recent_diffs_in_time_threshold == 0
        logging.debug(f"curr_idx = {curr_idx}, " +
              f"recent_diffs_in_point_threshold = {num_recent_diffs_in_point_threshold}, " +
              f"recent_diffs_in_time_threshold = {num_recent_diffs_in_time_threshold}")
        last_10_median_idx = np.median(np.arange(curr_idx - num_recent_diffs_in_point_threshold, curr_idx + 1)) # TODO weird but necessary to match the current behavior
        if ended_before_this:
            last_trip_end_index = int(last_10_median_idx)
            logging.debug(f"last_10_median_idx = {last_10_median_idx}, last_trip_end_index = {last_trip_end_index}")
        else:
            last_5min_median_idx = np.median(np.arange(curr_idx - num_recent_diffs_in_time_threshold, curr_idx))
            last_trip_end_index = int(min(last_5min_median_idx, last_10_median_idx))
            logging.debug(f"last_5min_median_idx = {last_5min_median_idx}, last_10_median_idx = {last_10_median_idx}, last_trip_end_index = {last_trip_end_index}")

        logging.info(f"ended_before_this = {ended_before_this}, last_trip_end_index = {last_trip_end_index}")
        return (ended_before_this, last_trip_end_index)
