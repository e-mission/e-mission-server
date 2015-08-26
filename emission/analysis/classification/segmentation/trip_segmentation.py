# Standard imports
import attrdict as ad
import numpy as np
import datetime as pydt

# Our imports
import emission.analysis.classification.cleaning.location_smoothing as ls
import emission.analysis.point_features as pf

def segment_into_trips(points_df):
    """
    What should the inputs be? Points for a day? All newly arrived points?
    Need to figure out how to hook this up to a pipeline. For now, assume that
    we have a set of points that we have identified.
    """
    filtered_accuracy_points_df = ls.add_speed(ls.filter_accuracy(points_df))
    filtered_points_df = filtered_accuracy_points_df[filtered_accuracy_points_df.distance != 0].reset_index(drop=True)

    segmentation_points = []
    last_trip_end_point = None
    curr_trip_start_point = None
    just_ended = True
    for idx, row in filtered_points_df.iterrows():
        currPoint = ad.AttrDict(row)
        currPoint.update({"idx": idx})
        print "-" * 30 + str(currPoint.formatted_time) + "-" * 30
        if curr_trip_start_point is None:
            print "Appending currPoint because the current start point is None"
            segmentation_points.append(currPoint)

        if just_ended:
            sel_point = currPoint
            print("Setting new trip start point %s with idx %s" % (sel_point, sel_point.idx))
            curr_trip_start_point = sel_point
            just_ended = False
            
        last5MinsPoints_df = filtered_points_df[np.logical_and(
                                                                np.logical_and(
                                                                        filtered_points_df.mTime > currPoint.mTime - 5 * 60 * 1000,
                                                                        filtered_points_df.mTime < currPoint.mTime),
                                                                filtered_points_df.mTime >= curr_trip_start_point.mTime)]
        # Using .loc here causes problems if we have filtered out some points and so the index is non-consecutive.
        # Using .iloc just ends up including points after this one.
        # So we reset_index upstream and use it here.
        # We are going to use the last 8 points for now.
        # TODO: Change this back to last 10 points once we normalize phone and this
        last10Points_df = filtered_points_df.iloc[max(idx-8, curr_trip_start_point.idx):idx+1]
        distanceToLast = lambda(row): pf.calDistance(ad.AttrDict(row), currPoint)
        last5MinsDistances = last5MinsPoints_df.apply(distanceToLast, axis=1)
        print "last5MinsDistances = %s with length %d" % (last5MinsDistances.as_matrix(), len(last5MinsDistances))
        last10PointsDistances = last10Points_df.apply(distanceToLast, axis=1)
        print "last10PointsDistances = %s with length %d, shape %s" % (last10PointsDistances.as_matrix(),
                                                                       len(last10PointsDistances),
                                                                       last10PointsDistances.shape)
        
        print("len(last10PointsDistances) = %d, len(last5MinsDistances) = %d" %
              (len(last10PointsDistances), len(last5MinsDistances)))
        if (len(last10PointsDistances) < 9 or len(last5MinsDistances) == 0):
            print "Too few points to make a decision, continuing"
        else:
            print("last5MinsDistances.max() = %s, last10PointsDistance.max() = %s" %
              (last5MinsDistances.max(), last10PointsDistances.max()))
            if (last5MinsDistances.max() < 100 and last10PointsDistances.max() < 100):
                last_trip_end_index = int(min(np.median(last5MinsPoints_df.index),
                                           np.median(last10Points_df.index)))
                print("last5MinPoints.median = %s (%s), last10Points_df = %s (%s), sel index = %s" %
                    (np.median(last5MinsPoints_df.index), last5MinsPoints_df.index,
                     np.median(last10Points_df.index), last10Points_df.index,
                     last_trip_end_index))
                last_trip_end_point = filtered_points_df.iloc[last_trip_end_index]
                print("Appending last_trip_end_point %s with index %s " % (last_trip_end_point, last_trip_end_point.name))
                segmentation_points.append(last_trip_end_point)
                print "Found trip end at %s" % str(pydt.datetime.fromtimestamp(last_trip_end_point.mTime/1000))
                just_ended = True
    return segmentation_points
