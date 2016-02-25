# Standard imports
import logging
import attrdict as ad
import numpy as np
import datetime as pydt

# Our imports
import emission.analysis.point_features as pf
import emission.analysis.intake.segmentation.trip_segmentation as eaist
import emission.core.wrapper.location as ecwl

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
        filtered_points_df = timeseries.get_data_df("background/filtered_location", time_query)
        transition_df = timeseries.get_data_df("statemachine/transition", time_query)

        if len(filtered_points_df) == 0:
            self.last_ts_processed = None
        else:
            # TODO: Decide whether we should return the write_ts in the entry,
            # or whether we should search by timestamp instead.
            # Depends on final direction for the timequery
            self.last_ts_processed = filtered_points_df.iloc[-1].metadata_write_ts

        logging.info("Last ts processed = %s" % self.last_ts_processed)

        segmentation_points = []
        last_trip_end_point = None
        curr_trip_start_point = None
        just_ended = True
        for idx, row in filtered_points_df.iterrows():
            currPoint = ad.AttrDict(row)
            currPoint.update({"idx": idx})
            logging.debug("-" * 30 + str(currPoint.fmt_time) + "-" * 30)
            if curr_trip_start_point is None:
                logging.debug("Appending currPoint because the current start point is None")
                # segmentation_points.append(currPoint)

            if just_ended:
                lastPoint = ad.AttrDict(filtered_points_df.iloc[idx-1])
                logging.debug("Comparing with lastPoint = %s, distance = %s, time = %s" % 
                    (lastPoint, pf.calDistance(lastPoint, currPoint) < self.distance_threshold,
                     currPoint.ts - lastPoint.ts <= self.time_threshold))
                # Unlike the time filter, with the distance filter, we concatenate all points
                # that are within the distance threshold with the previous trip
                # end, since because of the distance filter, even noisy points
                # can occur at an arbitrary time in the future
                if pf.calDistance(lastPoint, currPoint) < self.distance_threshold:
                    logging.info("Points %s and %s are within the distance filter so part of the same trip" %
                                 (lastPoint, currPoint))
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
                last10Points_df = filtered_points_df.iloc[max(idx-self.point_threshold, curr_trip_start_point.idx):idx+1]
                lastPoint = ad.AttrDict(filtered_points_df.iloc[idx-1])
                logging.debug("lastPoint = %s, time difference = %s dist difference %s" %
                    (lastPoint, currPoint.ts - lastPoint.ts, pf.calDistance(lastPoint, currPoint)))
                if currPoint.ts - lastPoint.ts > self.time_threshold:
                    # We have been at this location for more than the time filter.
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
                    last_trip_end_point = lastPoint
                    logging.debug("Appending last_trip_end_point %s with index %s " %
                        (last_trip_end_point, idx-1))
                    segmentation_points.append((curr_trip_start_point, last_trip_end_point))
                    logging.info("Found trip end at %s" % last_trip_end_point.fmt_time)
                    just_ended = True
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
        stopped_moving_after_last = transition_df[(transition_df.ts > currPoint.ts) & (transition_df.transition == 2)]
        if len(stopped_moving_after_last) > 0:
            segmentation_points.append((curr_trip_start_point, currPoint))
        return segmentation_points
