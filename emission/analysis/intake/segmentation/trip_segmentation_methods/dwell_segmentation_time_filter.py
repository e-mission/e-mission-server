# Standard imports
import attrdict as ad
import numpy as np
import datetime as pydt

# Our imports
import emission.analysis.point_features as pf
import emission.analysis.intake.segmentation.trip_segmentation as eaist
import emission.core.wrapper.location as ecwl

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
        filtered_points_df = timeseries.get_data_df("background/filtered_location", time_query)

        segmentation_points = []
        last_trip_end_point = None
        curr_trip_start_point = None
        just_ended = True
        for idx, row in filtered_points_df.iterrows():
            currPoint = ad.AttrDict(row)
            currPoint.update({"idx": idx})
            print "-" * 30 + str(currPoint.fmt_time) + "-" * 30
            if curr_trip_start_point is None:
                print "Appending currPoint because the current start point is None"
                # segmentation_points.append(currPoint)

            if just_ended:
                # Normally, at this point, since the logic here and the
                # logic on the phone are the same, if we have detected a trip
                # end, any points after this are part of the new trip.
                #
                #
                # However, in some circumstances, notably in my data from 27th
                # August, there appears to be a mismatch and we get a couple of
                # points past the end that we detected here.  So let's look for
                # points that are within the distance filter, and are at a
                # delta of 30 secs, and ignore them instead of using them to
                # start the new trip
                prev_point = filtered_points_df.iloc[idx - 1]
                print("Comparing with prev_point = %s" % prev_point)
                if pf.calDistance(prev_point, currPoint) < self.distance_threshold and \
                    currPoint.ts - prev_point.ts <= 60:
                    print("Points %s and %s are within the distance filter and only 1 min apart so part of the same trip" % (prev_point, currPoint))
                    continue
                # else: 
                sel_point = currPoint
                print("Setting new trip start point %s with idx %s" % (sel_point, sel_point.idx))
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
            distanceToLast = lambda(row): pf.calDistance(ad.AttrDict(row), currPoint)
            last5MinsDistances = last5MinsPoints_df.apply(distanceToLast, axis=1)
            print "last5MinsDistances = %s with length %d" % (last5MinsDistances.as_matrix(), len(last5MinsDistances))
            last10PointsDistances = last10Points_df.apply(distanceToLast, axis=1)
            print "last10PointsDistances = %s with length %d, shape %s" % (last10PointsDistances.as_matrix(),
                                                                           len(last10PointsDistances),
                                                                           last10PointsDistances.shape)
            
            print("len(last10PointsDistances) = %d, len(last5MinsDistances) = %d" %
                  (len(last10PointsDistances), len(last5MinsDistances)))
            if (len(last10PointsDistances) < self.point_threshold - 1 or len(last5MinsDistances) == 0):
                print "Too few points to make a decision, continuing"
            else:
                print("last5MinsDistances.max() = %s, last10PointsDistance.max() = %s" %
                  (last5MinsDistances.max(), last10PointsDistances.max()))
                if (last5MinsDistances.max() < self.distance_threshold and 
                    last10PointsDistances.max() < self.distance_threshold):
                    last_trip_end_index = int(min(np.median(last5MinsPoints_df.index),
                                               np.median(last10Points_df.index)))
                    print("last5MinPoints.median = %s (%s), last10Points_df = %s (%s), sel index = %s" %
                        (np.median(last5MinsPoints_df.index), last5MinsPoints_df.index,
                         np.median(last10Points_df.index), last10Points_df.index,
                         last_trip_end_index))
                    last_trip_end_point_row = filtered_points_df.iloc[last_trip_end_index]
                    last_trip_end_point = ad.AttrDict(filtered_points_df.iloc[last_trip_end_index])
                    print("Appending last_trip_end_point %s with index %s " % 
                        (last_trip_end_point, last_trip_end_point_row.name))
                    segmentation_points.append((curr_trip_start_point, last_trip_end_point))
                    print "Found trip end at %s" % last_trip_end_point.fmt_time
                    just_ended = True
        return segmentation_points
