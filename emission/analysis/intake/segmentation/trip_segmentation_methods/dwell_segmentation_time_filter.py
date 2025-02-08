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


TWELVE_HOURS = 12 * 60 * 60


def haversine(lon1, lat1, lon2, lat2):
    earth_radius = 6371000  # meters
    lat1, lat2 = np.radians(lat1), np.radians(lat2)
    lon1, lon2 = np.radians(lon1), np.radians(lon2)
    
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = np.sin(dlat / 2.0) ** 2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon / 2.0) ** 2
    return 2 * earth_radius * np.arcsin(np.sqrt(a))


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


    def segment_into_trips(self, timeseries, time_query, loc_df):
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

        self.transition_df = timeseries.get_data_df("statemachine/transition", time_query)
        self.motion_df = timeseries.get_data_df("background/motion_activity", time_query)

        if len(self.transition_df) > 0:
            logging.debug("self.transition_df = %s" % self.transition_df[["fmt_time", "transition"]])
        else:
            logging.debug("self.transition_df is empty")

        self.last_ts_processed = None
        logging.info("Last ts processed = %s" % self.last_ts_processed)

        loc_df['ts_diff'] = loc_df['ts'].diff()
        loc_df['last_10_dists'], loc_df['last_5min_dists'] = self.compute_distances(loc_df)
        loc_df['dist_diff'] = loc_df['last_10_dists'].apply(lambda x: x[-1])
        loc_df['speed_diff'] = loc_df['dist_diff'] / loc_df['ts_diff']
        loc_df['ongoing_motion'] = eaisr.ongoing_motion_in_loc_df(loc_df, self.motion_df)
        loc_df['tracking_restarted'] = eaisr.tracking_restarted_in_loc_df(loc_df, self.transition_df)
        
        segmentation_idxs = []
        last_segmented_idx = 0
        with ect.Timer() as t_loop:
            while last_segmented_idx < len(loc_df):
                print("last_segmented_idx = %s" % last_segmented_idx)
                # trim off dists of points that were before the last_segmented_idx
                last_10_dists_filtered = [
                    dists if row_idx - last_segmented_idx > len(dists)
                    else dists[last_segmented_idx - row_idx:]
                    for row_idx, dists in loc_df['last_10_dists'].items()
                ]
                last_5min_dists_filtered = [
                    dists if row_idx - last_segmented_idx > len(dists)
                    else dists[last_segmented_idx - row_idx:]
                    for row_idx, dists in loc_df['last_5min_dists'].items()
                ]

                last_10_max_dists = np.array(
                    [dists.max()
                        # if we don't have enough points, we can't make a decision
                        if len(dists) >= self.point_threshold - 2 # TODO weird but necessary to match the current behavior
                        else np.inf
                     for dists in last_10_dists_filtered]
                )
                last_5min_max_dists = np.array(
                    [dists.max()
                        # if we don't have points going back far enough, we can't make a decision
                        if loc_df.iloc[row_idx - len(dists)]['ts'] < loc_df.iloc[row_idx]['ts'] - (self.time_threshold - 30)
                        else np.inf
                     for row_idx, dists in enumerate(last_5min_dists_filtered)]
                )

                idxs = np.where(
                    # check points that haven't already been segmented
                    (loc_df.index > last_segmented_idx)
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
                        | ((last_10_max_dists < self.distance_threshold)
                           & (last_5min_max_dists < self.distance_threshold))
                    )
                )[0]

                logging.info(f'idxs = {idxs}')

                if len(idxs) == 0:
                    logging.info(f'No more segments, last_segmented_idx = {last_segmented_idx} / {len(loc_df)-1}')

                    if last_segmented_idx < len(loc_df) -1 and len(self.transition_df) > 0:
                        last_point_ts = loc_df.iloc[-1]['ts']
                        stopped_moving_after_last = self.transition_df[
                            (self.transition_df['ts'] > last_point_ts) & (self.transition_df['transition'] == 2)
                        ]
                        logging.info("looking after %s, found transitions %s" %
                                    (last_point_ts, stopped_moving_after_last))
                        if len(stopped_moving_after_last) > 0:
                            (_, last_trip_end_idx) = self.get_last_trip_end_point_idx(
                                len(loc_df) - 1,
                                last_10_dists_filtered[len(loc_df) - 1],
                                last_5min_dists_filtered[len(loc_df) - 1],
                            )
                            segmentation_idxs.append((last_segmented_idx, last_trip_end_idx))
                            logging.info(f'Found trip end at {last_trip_end_idx}')

                            self.last_ts_processed = float(loc_df.iloc[-1]['metadata_write_ts'])
                    last_segmented_idx = len(loc_df)
                    break

                trip_end_idx = idxs[0]
                logging.info(f'***** SEGMENTING AT {trip_end_idx} / {len(loc_df)-1} *****')

                index_to_print = trip_end_idx
                logging.info(f'last_10_dists_filtered[{index_to_print}] = {last_10_dists_filtered[index_to_print]}')
                logging.info(f'last_10_max_dists[{index_to_print}] = {last_10_max_dists[index_to_print]}')
                logging.info(f'last_5min_dists_filtered[{index_to_print}] = {last_5min_dists_filtered[index_to_print]}')
                logging.info(f'last_5min_max_dists[{index_to_print}] = {last_5min_max_dists[index_to_print]}')

                ended_before_this, last_trip_end_idx = self.get_last_trip_end_point_idx(
                    trip_end_idx,
                    last_10_dists_filtered[trip_end_idx],
                    last_5min_dists_filtered[trip_end_idx],
                )
                segmentation_idxs.append((last_segmented_idx, last_trip_end_idx))

                if ended_before_this:
                    # there was a big gap before trip_end_idx,
                    # which means it is actually the start of the next trip
                    last_segmented_idx = trip_end_idx
                    self.last_ts_processed = float(loc_df.iloc[last_segmented_idx]['metadata_write_ts'])
                    logging.info(f'set last_segmented_idx {self.last_ts_processed}')
                else:
                    # look for the next point that is outside the filter
                    next_start_idx = loc_df[
                        (loc_df.index > trip_end_idx) & (
                            (loc_df['ts_diff'] > 60)
                            | (loc_df['dist_diff'] >= self.distance_threshold)
                        )
                    ].index

                    if len(next_start_idx) > 0:
                        last_segmented_idx = next_start_idx[0]
                        logging.info(f'setting last_ts_processed to {last_segmented_idx-1} {loc_df.iloc[last_segmented_idx-1]["metadata_write_ts"]}')
                        self.last_ts_processed = float(loc_df.iloc[last_segmented_idx-1]['metadata_write_ts'])
                    elif trip_end_idx + 1 < len(loc_df):
                        last_segmented_idx = trip_end_idx + 1
                        logging.info(f'setting last_ts_processed to {last_segmented_idx} {loc_df.iloc[last_segmented_idx]["metadata_write_ts"]}')
                        self.last_ts_processed = float(loc_df.iloc[last_segmented_idx]['metadata_write_ts'])
                    else:
                        last_segmented_idx = len(loc_df)
                        logging.info(f'setting last_ts_processed to {len(loc_df) - 1} {loc_df.iloc[-1]["metadata_write_ts"]}')
                        self.last_ts_processed = float(loc_df.iloc[-1]['metadata_write_ts'])

        esds.store_pipeline_time(
            user_id,
            ecwp.PipelineStages.TRIP_SEGMENTATION.name + "/segment_into_trips_time/loop",
            time.time(),
            t_loop.elapsed
        )

        segmentation_points = [
            (ad.AttrDict(loc_df.iloc[last_segmented_idx]),
             ad.AttrDict(loc_df.iloc[last_trip_end_idx]))
            for (last_segmented_idx, last_trip_end_idx) in segmentation_idxs
        ]

        logging.info(f'self.last_ts_processed = {self.last_ts_processed}')
        logging.info("Returning segmentation_points:\n %s" % '\n'.join([
            f'{p1["index"]}, {p1["ts"]} -> {p2["index"]}, {p2["ts"]}'
            for (p1, p2) in segmentation_points]))
        
        return segmentation_points


    def compute_distances(self, loc_df):
        lat, lon = loc_df["latitude"].to_numpy(), loc_df["longitude"].to_numpy()
        timestamps = loc_df["ts"].to_numpy()
        indices = np.arange(len(loc_df))

        last_10_start_indices = np.searchsorted(indices, indices - self.point_threshold)
        last_10_distances = pd.Series([
            haversine(lon[last_10_start_indices[i]:i], lat[last_10_start_indices[i]:i], lon[i], lat[i])
            if last_10_start_indices[i] < i else np.empty(self.point_threshold)
            for i in range(len(loc_df))
        ])

        last_5min_start_indices = np.searchsorted(timestamps, timestamps - self.time_threshold, side='right')
        last_5min_distances = pd.Series([
            haversine(lon[last_5min_start_indices[i]:i], lat[last_5min_start_indices[i]:i], lon[i], lat[i])
            if last_5min_start_indices[i] < i else np.full(self.point_threshold, np.nan)
            for i in range(len(loc_df))
        ])

        return last_10_distances, last_5min_distances


    def get_last_trip_end_point_idx(self, curr_idx, last_10_dists, last_5min_dists):
        last_5min_dists_non_nan = last_5min_dists[~np.isnan(last_5min_dists)]
        ended_before_this = len(last_5min_dists_non_nan) == 0
        logging.info("ended_before_this = %s, curr_idx = %s, last_5min_dists_non_nan = %s " % (ended_before_this, curr_idx, last_5min_dists_non_nan))
        last_10_median_idx = np.median(np.arange(curr_idx - len(last_10_dists), curr_idx + 1)) # TODO weird but necessary to matche the current behavior
        if ended_before_this:
            last_trip_end_index = int(last_10_median_idx)
            logging.debug("last5MinsPoints not found, last_trip_end_index = %s" % last_trip_end_index)
        else:
            last_5min_median_idx = np.median(np.arange(curr_idx - len(last_5min_dists_non_nan), curr_idx))
            last_trip_end_index = int(min(last_5min_median_idx, last_10_median_idx))
            logging.debug("last5MinsPoints and last10PointsMedian found, last_trip_end_index = %s" % last_trip_end_index)

        logging.debug("ended_before_this = %s, last_trip_end_index = %s" % (ended_before_this, last_trip_end_index))
        return (ended_before_this, last_trip_end_index)

