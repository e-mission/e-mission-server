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
import datetime as pydt

# Our imports
import emission.analysis.point_features as pf
import emission.analysis.intake.segmentation.trip_segmentation as eaist
import emission.core.wrapper.location as ecwl

import emission.analysis.intake.segmentation.restart_checking as eaisr
import emission.analysis.intake.segmentation.trip_segmentation_methods.trip_end_detection_corner_cases as eaistc

class DwellSegmentationDistFilter(eaist.TripSegmentationMethod):
    def __init__(self, time_threshold, point_threshold, distance_threshold):
        """
        Determines segmentation points for points that were generated using a
        distance filter (i.e. report points every n meters). This will *not* work for
        points generated using a distance filter because it expects to have a
        time gap between subsequent points to detect the trip end, and with a
        time filter, we get updates every n seconds.

        At least on iOS, we sometimes get points even when the phone is not in
        motion. This seems to be triggered by zigzagging between low quality
        points.
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
        self.filtered_points_df = timeseries.get_data_df("background/filtered_location", time_query)
        self.filtered_points_df.loc[:,"valid"] = True
        self.transition_df = timeseries.get_data_df("statemachine/transition", time_query)
        if len(self.transition_df) > 0:
            logging.debug("self.transition_df = %s" % self.transition_df[["fmt_time", "transition"]])
        else:
            logging.debug("no transitions found. This can happen for continuous sensing")

        self.last_ts_processed = None

        logging.info("Last ts processed = %s" % self.last_ts_processed)

        segmentation_points = []
        last_trip_end_point = None
        curr_trip_start_point = None
        just_ended = True
        for idx, row in self.filtered_points_df.iterrows():
            currPoint = ad.AttrDict(row)
            currPoint.update({"idx": idx})
            logging.debug("-" * 30 + str(currPoint.fmt_time) + "-" * 30)
            if curr_trip_start_point is None:
                logging.debug("Appending currPoint because the current start point is None")
                # segmentation_points.append(currPoint)

            if just_ended:
                if self.continue_just_ended(idx, currPoint, self.filtered_points_df):
                    # We have "processed" the currPoint by deciding to glom it
                    self.last_ts_processed = currPoint.metadata_write_ts
                    continue
                # else: 
                # Here's where we deal with the start trip. At this point, the
                # distance is greater than the filter. 
                sel_point = currPoint
                logging.debug("Setting new trip start point %s with idx %s" % (sel_point, sel_point.idx))
                curr_trip_start_point = sel_point
                just_ended = False
            else:
                # Using .loc here causes problems if we have filtered out some points and so the index is non-consecutive.
                # Using .iloc just ends up including points after this one.
                # So we reset_index upstream and use it here.
                last10Points_df = self.filtered_points_df.iloc[max(idx-self.point_threshold, curr_trip_start_point.idx):idx+1]
                lastPoint = self.find_last_valid_point(idx)
                if self.has_trip_ended(lastPoint, currPoint, timeseries):
                    last_trip_end_point = lastPoint
                    logging.debug("Appending last_trip_end_point %s with index %s " %
                        (last_trip_end_point, idx-1))
                    segmentation_points.append((curr_trip_start_point, last_trip_end_point))
                    logging.info("Found trip end at %s" % last_trip_end_point.fmt_time)
                    # We have processed everything up to the trip end by marking it as a completed trip
                    self.last_ts_processed = currPoint.metadata_write_ts
                    just_ended = True
                    # Now, we have finished processing the previous point as a trip
                    # end or not. But we still need to process this point by seeing
                    # whether it should represent a new trip start, or a glom to the
                    # previous trip
                    if not self.continue_just_ended(idx, currPoint, self.filtered_points_df):
                        sel_point = currPoint
                        logging.debug("Setting new trip start point %s with idx %s" % (sel_point, sel_point.idx))
                        curr_trip_start_point = sel_point
                        just_ended = False

        # Since we only end a trip when we start a new trip, this means that
        # the last trip that was pushed is ignored. Consider the example of
        # 2016-02-22 when I took kids to karate. We arrived shortly after 4pm,
        # so during that remote push, a trip end was not detected. And we got
        # back home shortly after 5pm, so the trip end was only detected on the
        # phone at 6pm. At that time, the following points were pushed:
        # ..., 2016-02-22T16:04:02, 2016-02-22T16:49:34, 2016-02-22T16:49:50,
        # ..., 2016-02-22T16:57:04
        # Then, on the server, while iterating through the points, we detected
        # a trip end at 16:04, and a new trip start at 16:49. But we did not
        # detect the trip end at 16:57, because we didn't start a new point.
        # This has two issues:
        # - we won't see this trip until the next trip start, which may be on the next day
        # - we won't see this trip at all, because when we run the pipeline the
        # next time, we will only look at points from that time onwards. These
        # points have been marked as "processed", so they won't even be considered.

        # There are multiple potential fixes:
        # - we can mark only the completed trips as processed. This will solve (2) above, but not (1)
        # - we can mark a trip end based on the fact that we only push data
        # when a trip ends, so if we have data, it means that the trip has been
        # detected as ended on the phone.
        # This seems a bit fragile - what if we start pushing incomplete trip
        # data for efficiency reasons? Therefore, we also check to see if there
        # is a trip_end_detected in this timeframe after the last point. If so,
        # then we end the trip at the last point that we have.
        if not just_ended and len(self.transition_df) > 0:
            stopped_moving_after_last = self.transition_df[(self.transition_df.ts > currPoint.ts) & (self.transition_df.transition == 2)]
            logging.debug("stopped_moving_after_last = %s" % stopped_moving_after_last[["fmt_time", "transition"]])
            if len(stopped_moving_after_last) > 0:
                logging.debug("Found %d transitions after last point, ending trip..." % len(stopped_moving_after_last))
                segmentation_points.append((curr_trip_start_point, currPoint))
                self.last_ts_processed = currPoint.metadata_write_ts
            else:
                logging.debug("Found %d transitions after last point, not ending trip..." % len(stopped_moving_after_last))
        return segmentation_points

    def has_trip_ended(self, lastPoint, currPoint, timeseries):
        # So we must not have been moving for the last _time filter_
        # points. So the trip must have ended
        # Since this is a distance filter, we detect that the last
        # trip has ended at the time that the new trip starts. So
        # if the last_trip_end_point is lastPoint, then
        # curr_trip_start_point should be currPoint. But then we will
        # have problems with the spurious, noisy points that are
        # generated until the geofence is turned on, if ever
        # So we will continue to defer new trip starting until we
        # have worked through all of those.
        timeDelta = currPoint.ts - lastPoint.ts
        distDelta = pf.calDistance(lastPoint, currPoint)
        logging.debug("lastPoint = %s, time difference = %s dist difference %s" %
                      (lastPoint, timeDelta, distDelta))
        if timeDelta > self.time_threshold:
            # We have been at this location for more than the time filter.
            # This could be because we have not been moving for the last
            # _time filter_ points, or because we didn't get points for
            # that duration, (e.g. because we were underground)
            if timeDelta > 0:
                speedDelta = old_div(distDelta, timeDelta)
            else:
                speedDelta = np.nan
            # this is way too slow. On ios, we use 5meters in 10 minutes.
            # On android, we use 10 meters in 5 mins, which seems to work better
            # for this kind of test
            speedThreshold = old_div(float(self.distance_threshold * 2), (old_div(self.time_threshold, 2)))

            if eaisr.is_tracking_restarted_in_range(lastPoint.ts, currPoint.ts, timeseries):
                logging.debug("tracking was restarted, ending trip")
                return True

            # In general, we get multiple locations between each motion activity. If we see a bunch of motion activities
            # between two location points, and there is a large gap between the last location and the first
            # motion activity as well, let us just assume that there was a restart
            ongoing_motion_in_range = eaisr.get_ongoing_motion_in_range(lastPoint.ts, currPoint.ts, timeseries)
            ongoing_motion_check = len(ongoing_motion_in_range) > 0
            if timeDelta > self.time_threshold and not ongoing_motion_check:
                logging.debug("lastPoint.ts = %s, currPoint.ts = %s, threshold = %s, large gap = %s, ongoing_motion_in_range = %s, ending trip" %
                              (lastPoint.ts, currPoint.ts,self.time_threshold, currPoint.ts - lastPoint.ts, ongoing_motion_check))
                return True

            # http://www.huffingtonpost.com/hoppercom/the-worlds-20-longest-non-stop-flights_b_5994268.html
            # Longest flight is 17 hours, which is the longest you can go without cell reception
            # And even if you split an air flight that long into two, you will get some untracked time in the
            # middle, so that's good.
            TWELVE_HOURS = 12 * 60 * 60
            if timeDelta > TWELVE_HOURS:
                logging.debug("lastPoint.ts = %s, currPoint.ts = %s, TWELVE_HOURS = %s, large gap = %s, ending trip" %
                              (lastPoint.ts, currPoint.ts, TWELVE_HOURS, currPoint.ts - lastPoint.ts))
                return True

            if (timeDelta > self.time_threshold and # We have been here for a while
                        speedDelta < speedThreshold): # we haven't moved very much
                # This can happen even during ongoing trips due to spurious points
                # generated on some iOS phones
                # https://github.com/e-mission/e-mission-server/issues/577#issuecomment-376379460
                if eaistc.is_huge_invalid_ts_offset(self, lastPoint, currPoint,
                    timeseries, ongoing_motion_in_range):
                    # delete from memory and the database. Should be generally
                    # discouraged, so we are kindof putting it in here
                    # secretively
                    logging.debug("About to set valid column for index = %s" % 
                        (currPoint.idx))
                    self.filtered_points_df.valid.iloc[currPoint.idx] = False
                    logging.debug("After dropping %d, filtered points = %s" % 
                        (currPoint.idx, self.filtered_points_df.iloc[currPoint.idx - 5:currPoint.idx + 5][["valid", "fmt_time"]]))
                    import emission.core.get_database as edb
                    logging.debug("remove huge invalid ts offset point = %s" % currPoint)
                    edb.get_timeseries_db().remove({"_id": currPoint["_id"]})
                    # We currently re-retrieve the last point every time, so
                    # the reindexing above is good enough but if we use
                    # lastPoint = currPoint, we should update currPoint here
                    return False
                else:
                    logging.debug("lastPoint.ts = %s, currPoint.ts = %s, threshold = %s, large gap = %s, ending trip" %
                                  (lastPoint.ts, currPoint.ts,self.time_threshold, currPoint.ts - lastPoint.ts))
                    return True
            else:
                logging.debug("lastPoint.ts = %s, currPoint.ts = %s, time gap = %s (vs %s), distance_gap = %s (vs %s), speed_gap = %s (vs %s) continuing trip" %
                              (lastPoint.ts, currPoint.ts,
                               timeDelta, self.time_threshold,
                               distDelta, self.distance_threshold,
                               speedDelta, speedThreshold))
                return False

    def find_last_valid_point(self, idx):
        lastPoint = ad.AttrDict(self.filtered_points_df.iloc[idx-1])
        if lastPoint.valid:
            # common case, fast
            return lastPoint

        # uncommon case, walk backwards until you find something valid.
        i = 2
        while not lastPoint.valid and (idx - i) >= 0:
            lastPoint = ad.AttrDict(self.filtered_points_df.iloc[idx-i])
            i = i-1
        return lastPoint

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
            lastPoint = ad.AttrDict(filtered_points_df.iloc[idx - 1])

            deltaDist = pf.calDistance(lastPoint, currPoint)
            deltaTime = currPoint.ts - lastPoint.ts
            logging.debug("Comparing with lastPoint = %s, distance = %s < threshold %s, time = %s < threshold %s" %
                          (lastPoint, deltaDist, self.distance_threshold,
                            deltaTime, self.time_threshold))
            # Unlike the time filter, with the distance filter, we concatenate all points
            # that are within the distance threshold with the previous trip
            # end, since because of the distance filter, even noisy points
            # can occur at an arbitrary time in the future
            if deltaDist < self.distance_threshold:
                logging.info("Points %s (%s) and %s (%s) are %d apart, within the distance filter so part of the same trip" %
                             (lastPoint["_id"], lastPoint.loc, currPoint["_id"], currPoint.loc, deltaDist))
                return True
            else:
                return False

