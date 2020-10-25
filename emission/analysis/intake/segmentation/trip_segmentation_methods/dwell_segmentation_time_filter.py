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

# Our imports
import emission.analysis.point_features as pf
import emission.analysis.intake.segmentation.trip_segmentation as eaist
import emission.core.wrapper.location as ecwl

import emission.analysis.intake.segmentation.restart_checking as eaisr

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

    def segment_into_trips(self, timeseries, time_query):
        """
        Examines the timeseries database for a specific range and returns the
        segmentation points. Note that the input is the entire timeseries and
        the time range. This allows algorithms to use whatever combination of
        data that they want from the sensor streams in order to determine the
        segmentation points.
        """
        filtered_points_pre_ts_diff_df = timeseries.get_data_df("background/filtered_location", time_query)
        # Sometimes, we can get bogus points because data.ts and
        # metadata.write_ts are off by a lot. If we don't do this, we end up
        # appearing to travel back in time
        # https://github.com/e-mission/e-mission-server/issues/457
        filtered_points_df = filtered_points_pre_ts_diff_df[(filtered_points_pre_ts_diff_df.metadata_write_ts - filtered_points_pre_ts_diff_df.ts) < 1000]
        filtered_points_df.reset_index(inplace=True)
        transition_df = timeseries.get_data_df("statemachine/transition", time_query)
        if len(transition_df) > 0:
            logging.debug("transition_df = %s" % transition_df[["fmt_time", "transition"]])
        else:
            logging.debug("no transitions found. This can happen for continuous sensing")

        self.last_ts_processed = None

        logging.info("Last ts processed = %s" % self.last_ts_processed)

        segmentation_points = []
        last_trip_end_point = None
        curr_trip_start_point = None
        just_ended = True
        prevPoint = None
        for idx, row in filtered_points_df.iterrows():
            currPoint = ad.AttrDict(row)
            currPoint.update({"idx": idx})
            logging.debug("-" * 30 + str(currPoint.fmt_time) + "-" * 30)
            if curr_trip_start_point is None:
                logging.debug("Appending currPoint because the current start point is None")
                # segmentation_points.append(currPoint)

            if just_ended:
                if self.continue_just_ended(idx, currPoint, filtered_points_df):
                    # We have "processed" the currPoint by deciding to glom it
                    self.last_ts_processed = currPoint.metadata_write_ts
                    continue
                # else:
                sel_point = currPoint
                logging.debug("Setting new trip start point %s with idx %s" % (sel_point, sel_point.idx))
                curr_trip_start_point = sel_point
                just_ended = False

            last5MinsPoints_df = filtered_points_df[np.logical_and(
                                                        np.logical_and(
                                                                filtered_points_df.ts > currPoint.ts - self.time_threshold,
                                                                filtered_points_df.ts < currPoint.ts),
                                                        filtered_points_df.ts >= curr_trip_start_point.ts)]
            # Using .loc here causes problems if we have filtered out some points and so the index is non-consecutive.
            # Using .iloc just ends up including points after this one.
            # So we reset_index upstream and use it here.
            # We are going to use the last 8 points for now.
            # TODO: Change this back to last 10 points once we normalize phone and this
            last10Points_df = filtered_points_df.iloc[max(idx-self.point_threshold, curr_trip_start_point.idx):idx+1]
            distanceToLast = lambda row: pf.calDistance(ad.AttrDict(row), currPoint)
            timeToLast = lambda row: currPoint.ts - ad.AttrDict(row).ts
            last5MinsDistances = last5MinsPoints_df.apply(distanceToLast, axis=1)
            logging.debug("last5MinsDistances = %s with length %d" % (last5MinsDistances.to_numpy(), len(last5MinsDistances)))
            last10PointsDistances = last10Points_df.apply(distanceToLast, axis=1)
            logging.debug("last10PointsDistances = %s with length %d, shape %s" % (last10PointsDistances.to_numpy(),
                                                                           len(last10PointsDistances),
                                                                           last10PointsDistances.shape))

            # Fix for https://github.com/e-mission/e-mission-server/issues/348
            last5MinTimes = last5MinsPoints_df.apply(timeToLast, axis=1)
            
            logging.debug("len(last10PointsDistances) = %d, len(last5MinsDistances) = %d" %
                  (len(last10PointsDistances), len(last5MinsDistances)))
            logging.debug("last5MinsTimes.max() = %s, time_threshold = %s" %
                          (last5MinTimes.max() if len(last5MinTimes) > 0 else np.NaN, self.time_threshold))

            if self.has_trip_ended(prevPoint, currPoint, timeseries, last10PointsDistances, last5MinsDistances, last5MinTimes):
                (ended_before_this, last_trip_end_point) = self.get_last_trip_end_point(filtered_points_df,
                                                                                       last10Points_df, last5MinsPoints_df)
                segmentation_points.append((curr_trip_start_point, last_trip_end_point))
                logging.info("Found trip end at %s" % last_trip_end_point.fmt_time)
                # We have processed everything up to the trip end by marking it as a completed trip
                self.last_ts_processed = currPoint.metadata_write_ts
                if ended_before_this:
                    # in this case, we end a trip at the previous point, and the next trip starts at this
                    # point, not the next one
                    just_ended = False
                    prevPoint = currPoint
                    curr_trip_start_point = currPoint
                    logging.debug("Setting new trip start point %s with idx %s" %
                                  (currPoint, currPoint.idx))
                else:
                    # We end a trip at the current point, and the next trip starts at the next point
                    just_ended = True
                    prevPoint = None
            else:
                prevPoint = currPoint

        logging.debug("Iterated over all points, just_ended = %s, len(transition_df) = %s" %
                      (just_ended, len(transition_df)))
        if not just_ended and len(transition_df) > 0:
            stopped_moving_after_last = transition_df[(transition_df.ts > currPoint.ts) & (transition_df.transition == 2)]
            logging.debug("looking after %s, found transitions %s" %
                          (currPoint.ts, stopped_moving_after_last))
            if len(stopped_moving_after_last) > 0:
                (unused, last_trip_end_point) = self.get_last_trip_end_point(filtered_points_df,
                                                                             last10Points_df, None)
                segmentation_points.append((curr_trip_start_point, last_trip_end_point))
                logging.debug("Found trip end at %s" % last_trip_end_point.fmt_time)
                # We have processed everything up to the trip end by marking it as a completed trip
                self.last_ts_processed = currPoint.metadata_write_ts

        return segmentation_points

    def continue_just_ended(self, idx, currPoint, filtered_points_df):
        """
        Normally, since the logic here and the
        logic on the phone are the same, if we have detected a trip
        end, any points after this are part of the new trip.

        However, in some circumstances, notably in my data from 27th
        August, there appears to be a mismatch and we get a couple of
        points past the end that we detected here.  So let's look for
        points that are within the distance filter, and are at a
        delta of a minute, and join them to the just ended trip instead of using them to
        start the new trip

        :param idx: Index of the current point
        :param currPoint: current point
        :param filtered_points_df: dataframe of filtered points
        :return: True if we should continue the just ended trip, False otherwise
        """
        if idx == 0:
            return False
        else:
            prev_point = ad.AttrDict(filtered_points_df.iloc[idx - 1])
            logging.debug("Comparing with prev_point = %s" % prev_point)
            if pf.calDistance(prev_point, currPoint) < self.distance_threshold and \
                                    currPoint.ts - prev_point.ts <= 60:
                logging.info("Points %s and %s are within the distance filter and only 1 min apart so part of the same trip" %
                             (prev_point, currPoint))
                return True
            else:
                return False

    def has_trip_ended(self, prev_point, curr_point, timeseries, last10PointsDistances, last5MinsDistances, last5MinTimes):
        # Another mismatch between phone and server. Phone stops tracking too soon,
        # so the distance is still greater than the threshold at the end of the trip.
        # But then the next point is a long time away, so we can split again (similar to a distance filter)
        if prev_point is None:
            logging.debug("prev_point is None, continuing trip")
        else:
            timeDelta = curr_point.ts - prev_point.ts
            distDelta = pf.calDistance(prev_point, curr_point)
            if timeDelta > 0:
                speedDelta = old_div(distDelta, timeDelta)
            else:
                speedDelta = np.nan
            speedThreshold = old_div(float(self.distance_threshold), self.time_threshold)

            if eaisr.is_tracking_restarted_in_range(prev_point.ts, curr_point.ts, timeseries):
                logging.debug("tracking was restarted, ending trip")
                return True

            ongoing_motion_check = len(eaisr.get_ongoing_motion_in_range(prev_point.ts, curr_point.ts, timeseries)) > 0
            if timeDelta > 2 * self.time_threshold and not ongoing_motion_check:
                logging.debug("lastPoint.ts = %s, currPoint.ts = %s, threshold = %s, large gap = %s, ongoing_motion_in_range = %s, ending trip" %
                              (prev_point.ts, curr_point.ts,self.time_threshold, curr_point.ts - prev_point.ts, ongoing_motion_check))
                return True

            # http://www.huffingtonpost.com/hoppercom/the-worlds-20-longest-non-stop-flights_b_5994268.html
            # Longest flight is 17 hours, which is the longest you can go without cell reception
            # And even if you split an air flight that long into two, you will get some untracked time in the
            # middle, so that's good.
            TWELVE_HOURS = 12 * 60 * 60
            if timeDelta > TWELVE_HOURS:
                logging.debug("prev_point.ts = %s, curr_point.ts = %s, TWELVE_HOURS = %s, large gap = %s, ending trip" %
                              (prev_point.ts, curr_point.ts, TWELVE_HOURS, curr_point.ts - prev_point.ts))
                return True

            if (timeDelta > 2 * self.time_threshold and # We have been here for a while
                 speedDelta < speedThreshold): # we haven't moved very much
                logging.debug("prev_point.ts = %s, curr_point.ts = %s, threshold = %s, large gap = %s, ending trip" %
                              (prev_point.ts, curr_point.ts,self.time_threshold, curr_point.ts - prev_point.ts))
                return True
            else:
                logging.debug("prev_point.ts = %s, curr_point.ts = %s, time gap = %s (vs %s), distance_gap = %s (vs %s), speed_gap = %s (vs %s) continuing trip" %
                              (prev_point.ts, curr_point.ts,
                               timeDelta, self.time_threshold,
                               distDelta, self.distance_threshold,
                               speedDelta, speedThreshold))

        # The -30 is a fuzz factor intended to compensate for older clients
        # where data collection stopped after 5 mins, so that we never actually
        # see 5 mins of data

        if (len(last10PointsDistances) < self.point_threshold - 1 or
                    len(last5MinsDistances) == 0 or
                    last5MinTimes.max() < self.time_threshold - 30):
            logging.debug("Too few points to make a decision, continuing")
            return False

        # Normal end-of-trip case
        logging.debug("last5MinsDistances.max() = %s, last10PointsDistance.max() = %s" %
                      (last5MinsDistances.max(), last10PointsDistances.max()))
        if (last5MinsDistances.max() < self.distance_threshold and
            last10PointsDistances.max() < self.distance_threshold):
                return True


    def get_last_trip_end_point(self, filtered_points_df, last10Points_df, last5MinsPoints_df):
        ended_before_this = last5MinsPoints_df is None or len(last5MinsPoints_df) == 0
        if ended_before_this:
            logging.debug("trip end transition, so last 10 points are %s" % last10Points_df.index)
            last10PointsMedian = np.median(last10Points_df.index)
            last_trip_end_index = int(last10PointsMedian)
            logging.debug("last5MinsPoints not found, last_trip_end_index = %s" % last_trip_end_index)
        else:
            last10PointsMedian = np.median(last10Points_df.index)
            last5MinsPointsMedian = np.median(last5MinsPoints_df.index)
            last_trip_end_index = int(min(last5MinsPointsMedian, last10PointsMedian))
            logging.debug("last5MinsPoints and last10PointsMedian found, last_trip_end_index = %s" % last_trip_end_index)
        #                     logging.debug("last5MinPoints.median = %s (%s), last10Points_df = %s (%s), sel index = %s" %
        #                         (np.median(last5MinsPoints_df.index), last5MinsPoints_df.index,
        #                          np.median(last10Points_df.index), last10Points_df.index,
        #                          last_trip_end_index))

        last_trip_end_point_row = filtered_points_df.iloc[last_trip_end_index]
        last_trip_end_point = ad.AttrDict(filtered_points_df.iloc[last_trip_end_index])
        logging.debug("Appending last_trip_end_point %s with index %s " %
                      (last_trip_end_point, last_trip_end_point_row.name))
        return (ended_before_this, last_trip_end_point)


