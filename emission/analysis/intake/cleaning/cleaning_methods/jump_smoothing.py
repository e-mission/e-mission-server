from __future__ import print_function
from __future__ import division
from __future__ import unicode_literals
from __future__ import absolute_import
# Techniques to smooth jumps in location tracking. Each of these returns a
# boolean mask of inliers and outliers. We assume that the incoming dataframe
# has a column called "speed" that represents the speed at each point. The
# speed of the first point is zero.
# The result is in the inlier_mask field of the appropriate object

# Standard imports
from future import standard_library
standard_library.install_aliases()
from builtins import zip
from builtins import *
from past.utils import old_div
from builtins import object
import logging
import math
import pandas as pd
import numpy as np
import attrdict as ad
from enum import Enum

import emission.analysis.point_features as pf
import emission.core.common as ec
# logging.basicConfig(level=logging.DEBUG)

class SmoothBoundary(object):
    def __init__(self, maxSpeed = 100):
        self.maxSpeed = maxSpeed

    def filter(self, with_speeds_df):
        self.inlier_mask_ = [True] * with_speeds_df.shape[0]

        prev_pt = None
        for (i, pt) in enumerate(with_speeds_df[["mLatitude", "mLongitude", "mTime", "speed"]].to_dict('records')):
            pt = ad.AttrDict(dict(pt))
            if prev_pt is None:
                # Don't have enough data yet, so don't make any decisions
                prev_pt = pt
            else:
                currSpeed = pf.calSpeed(prev_pt, pt)
                logging.debug("while considering point %s(%s), prev_pt (%s) speed = %s" % (pt, i, prev_pt, currSpeed))
                if currSpeed > self.maxSpeed:
                    logging.debug("currSpeed > %s, removing index %s " % (self.maxSpeed, i))
                    self.inlier_mask_[i] = False
                else:
                    logging.debug("currSpeed < %s, retaining index %s " % (self.maxSpeed, i))
                    prev_pt = pt
        logging.info("Filtering complete, removed indices = %s" % np.nonzero(self.inlier_mask_))

# We intentionally don't use a dataframe for the segment list, using a
# segment class instead. The reasons are as follows:
# 1. We don't really need any of the fancy row, column or matrix
# operations. We are going to iterate over the list one step at a time.
# 2. We have potentially have to resplit segments, which involves inserting
# elements at arbitrary points in the list. With a dataframe, we would need
# to reindex every time we did this, which we won't have to do if we use a
# simple list. It should be trivial to re-implement this as a DataFrame if
# we so choose.
# This is only used by SmoothZigzag currently. I had originally made this an
# inner class, but inner classes in python are not special so let's move it out
# and make the indenting less complicated
class Segment(object):
    State = Enum("Segment_State", "UNKNOWN BAD GOOD")
    CLUSTER_RADIUS = 100

    def __init__(self, start, end, zigzag_algo):
        self.start = start
        self.end = end
        self.state = Segment.State.UNKNOWN
        self.point_count = end - start
        self.za = zigzag_algo
        self.segment_df = self.za.with_speeds_df[start:end]
        self.distance = self.za.cal_distance(self)
        self.is_cluster = (self.distance < Segment.CLUSTER_RADIUS)
        logging.debug("For cluster %s - %s, distance = %s, is_cluster = %s" % 
            (self.start, self.end, self.distance, self.is_cluster))

    def __repr__(self):
        return "Segment(%s, %s, %s)" % (self.start, self.end, self.distance)

class SmoothZigzag(object):
    Direction = Enum("IterationDirection", 'RIGHT LEFT')

    @staticmethod
    def end_points_distance(segment):
        if segment.start == segment.end:
            raise RuntimeError("This is messed up segment. Investigate further")
        return pf.calDistance(segment.segment_df.iloc[0], segment.segment_df.iloc[-1])

    @staticmethod
    def shortest_non_cluster_segment(segment_list):
        assert(len(segment_list) > 0)
        segment_distance_list = [[segment.distance, segment.is_cluster] for segment in segment_list]
        segment_distance_df = pd.DataFrame(segment_distance_list, columns = ["distance", "is_cluster"])
        non_cluster_segments = segment_distance_df[segment_distance_df.is_cluster == False]
        if len(non_cluster_segments) == 0:
            # If every segment is a cluster, then it is very hard to
            # distinguish between them for zigzags. Let us see if there is any
            # one point cluster - i.e. where the distance is zero. If so, that is likely
            # to be a bad cluster, so we return the one to the right or left of it
            minDistanceCluster = segment_distance_df.distance.argmin()
            if minDistanceCluster == 0:
                goodCluster = minDistanceCluster + 1
                assert(goodCluster < len(segment_list))
                return goodCluster
            else:
                goodCluster = minDistanceCluster - 1
                assert(goodCluster >= 0)
                return goodCluster
        retVal = non_cluster_segments.distance.argmin()
        logging.debug("shortest_non_cluster_segment = %s" % retVal)
        return retVal

    def __init__(self, is_ios, same_point_distance, maxSpeed = 100):
        self.is_ios = is_ios
        self.same_point_distance = same_point_distance
        self.maxSpeed = maxSpeed
        self.cal_distance = self.end_points_distance
        self.find_start_segment = self.shortest_non_cluster_segment

    def find_segments(self):
        if self.is_ios:
            segmentation_points = self.get_segmentation_points_ios()
        else:
            segmentation_points = self.get_segmentation_points_android()

        segmentation_points.insert(0, 0)
        last_point = self.with_speeds_df.shape[0]
        if last_point not in segmentation_points:
            logging.debug("smoothing: last_point index %s not in found points %s" %
                          (last_point, segmentation_points))
            segmentation_points.insert(len(segmentation_points), last_point)
            logging.debug("smoothing: added new entry %s" % segmentation_points[-1])

        self.segment_list = [Segment(start, end, self) for (start, end) in 
                                zip(segmentation_points, segmentation_points[1:])]
        logging.debug("smoothing: segment_list = %s" % self.segment_list)

    def get_segmentation_points_android(self):
        return self.with_speeds_df[self.with_speeds_df.speed > self.maxSpeed].index.tolist()

    def get_segmentation_points_ios(self):
        jump_indices = self.with_speeds_df[self.with_speeds_df.speed > self.maxSpeed].index
        # On iOS, as seen in ...., this is likely to be the jump back. We now need to find
        # the jump to
        jumps = self.with_speeds_df[(self.with_speeds_df.speed > self.maxSpeed) &
                                    (self.with_speeds_df.distance > 100)].index
        logging.debug("After first step, jumps = %s" % jumps)
        all_jumps = []
        for jump in jumps.tolist():
            jump_to = self.with_speeds_df[(self.with_speeds_df.index < jump) & (
                self.with_speeds_df.distance > 100)].index[-1]
            logging.debug("for jump %s, jump_to = %s" % (jump, jump_to))
            all_jumps.append(jump_to)
            all_jumps.append(jump)
        logging.debug("for ios, returning all_jumps = %s" % all_jumps)
        return all_jumps

    def split_segment(self, i, curr_seg, direction):
        import emission.analysis.intake.cleaning.location_smoothing as ls

        if direction == SmoothZigzag.Direction.RIGHT:
            recomputed_speed_df = ls.recalc_speed(curr_seg.segment_df)
            # Find the first point that does not belong to the cluster
            new_split_point = recomputed_speed_df[recomputed_speed_df.distance > Segment.CLUSTER_RADIUS].index[0]
            new_seg = Segment(new_split_point, curr_seg.end, self)
            replace_seg = Segment(curr_seg.start, new_split_point, self)
            self.segment_list[i] = replace_seg
            self.segment_list.insert(i+1, new_seg)
            return replace_seg

        if direction == SmoothZigzag.Direction.LEFT:
            # Need to compute speeds and distances from the left edge
            recomputed_speed_df = ls.recalc_speed(curr_seg.segment_df.iloc[::-1])
            logging.debug("Recomputed_speed_df = %s", recomputed_speed_df.speed)
            # Find the first point that does not belong to the cluster
            new_split_point = recomputed_speed_df[recomputed_speed_df.distance > Segment.CLUSTER_RADIUS].index[0]
            logging.debug("new split point = %s", new_split_point)
            new_seg = Segment(curr_seg.start, new_split_point + 1, self)
            replace_seg = Segment(new_split_point + 1, curr_seg.end, self)
            self.segment_list[i] = replace_seg
            self.segment_list.insert(i, new_seg)
            return replace_seg

    @staticmethod
    def toggle(expected_state):
        assert expected_state == Segment.State.BAD or expected_state == Segment.State.GOOD, "Unable to toggle %s " % expected_state
        if expected_state == Segment.State.BAD:
            return Segment.State.GOOD
        if expected_state == Segment.State.GOOD:
            return Segment.State.BAD

    def mark_segment_states(self, start_segment_idx, direction):
        """
        This is the most complicated part of the algorithm.
        """
        if direction == SmoothZigzag.Direction.RIGHT:
            inc = 1
            check = lambda i: i < len(self.segment_list)
        if direction == SmoothZigzag.Direction.LEFT:
            inc = -1
            check = lambda i: i >= 0

        i = start_segment_idx + inc
        expected_state = Segment.State.BAD

        while(check(i)):
            curr_seg = self.segment_list[i]
            logging.debug("Processing segment %d: %s, expecting state %s" % (i, curr_seg, expected_state))
            assert curr_seg.state == Segment.State.UNKNOWN, "Attempting to overwite state for segment %s, curr state is %s" % (i, curr_seg.state)

            if expected_state == Segment.State.BAD and not curr_seg.is_cluster: # mixed cluster case
                curr_seg = self.split_segment(i, curr_seg, direction)
                # We inserted a new segment before this, so this is now moved down by one.
                # When we are moving right, we insert after this one, so the
                # current index is not affected
                if (direction == SmoothZigzag.Direction.LEFT):
                    i = i + 1
                logging.debug("Finishing process for %s after splitting mixed cluster"% curr_seg)
                assert curr_seg.is_cluster, "after splitting, the segment is not a cluster?!"
                # In the mixed case, we just inserted an element, so we don't
                # want to increment, because otherwise we will terminate too
                # early. Note that the end conditions are embedded in the
                # closure

            i = i + inc
            curr_seg.state = expected_state
            expected_state = SmoothZigzag.toggle(expected_state)
            logging.debug("At the end of the loop for direction %s, i = %s" % (direction, i))

        logging.debug("Finished marking segment states for direction %s " % direction)

    def filter(self, with_speeds_df):
        self.inlier_mask_ = pd.Series([True] * with_speeds_df.shape[0])
        self.with_speeds_df = with_speeds_df
        self.find_segments()
        logging.debug("After splitting, segment list is %s with size %s" % 
                (self.segment_list, len(self.segment_list)))
        if len(self.segment_list) == 1:
            # there were no jumps, so there's nothing to do
            logging.info("No jumps, nothing to filter")
            return
        start_segment_idx = self.find_start_segment(self.segment_list)
        self.segment_list[start_segment_idx].state = Segment.State.GOOD
        self.mark_segment_states(start_segment_idx, SmoothZigzag.Direction.RIGHT)
        self.mark_segment_states(start_segment_idx, SmoothZigzag.Direction.LEFT)
        unknown_segments = [segment for segment in self.segment_list if segment.state == Segment.State.UNKNOWN]
        logging.debug("unknown_segments = %s" % unknown_segments)
        assert len(unknown_segments) == 0, "Found %s unknown segments - early termination of loop?" % len(unknown_segments)
        bad_segments = [segment for segment in self.segment_list if segment.state == Segment.State.BAD]
        logging.debug("bad_segments = %s" % bad_segments)
        for segment in bad_segments:
            self.inlier_mask_[segment.start:segment.end] = False

        logging.debug("after setting values, outlier_mask = %s" % np.nonzero(self.inlier_mask_ == False))
        # logging.debug("point details are %s" % with_speeds_df[np.logical_not(self.inlier_mask_)])

        # TODO: This is not the right place for this - adds too many dependencies
        # Should do this in the outer class in general so that we can do
        # multiple passes of any filtering algorithm
        import emission.analysis.intake.cleaning.cleaning_methods.speed_outlier_detection as cso
        import emission.analysis.intake.cleaning.location_smoothing as ls

        recomputed_speeds_df = ls.recalc_speed(self.with_speeds_df[self.inlier_mask_])
        recomputed_threshold = cso.BoxplotOutlier(ignore_zeros = True).get_threshold(recomputed_speeds_df)
        # assert recomputed_speeds_df[recomputed_speeds_df.speed > recomputed_threshold].shape[0] == 0, "After first round, still have outliers %s" % recomputed_speeds_df[recomputed_speeds_df.speed > recomputed_threshold] 
        if recomputed_speeds_df[recomputed_speeds_df.speed > recomputed_threshold].shape[0] != 0:
            logging.info("After first round, still have outliers %s" % recomputed_speeds_df[recomputed_speeds_df.speed > recomputed_threshold])


class SmoothPosdap(object):
    def __init__(self, maxSpeed = 100):
        self.maxSpeed = maxSpeed

    def filter(self, with_speeds_df):
        self.inlier_mask_ = [True] * with_speeds_df.shape[0]

        quality_segments = []
        curr_segment = []
        prev_pt = None

        for (i, pt) in enumerate(with_speeds_df.to_dict('records')):
            pt = ad.AttrDict(pt)
            if prev_pt is None:
                # Don't have enough data yet, so don't make any decisions
                prev_pt = pt
            else:
                currSpeed = pf.calSpeed(prev_pt, pt)
                print("while considering point %s, speed = %s" % (i, currSpeed))
                # Should make this configurable
                if currSpeed > self.maxSpeed:
                    print("currSpeed > %d, starting new quality segment at index %s " % (self.maxSpeed, i))
                    quality_segments.append(curr_segment)
                    curr_segment = []
                else:
                    print("currSpeed < %d, retaining index %s in existing quality segment " % (self.maxSpeed, i))
                prev_pt = pt
                curr_segment.append(i)
        # Append the last segment once we are at the end
        quality_segments.append(curr_segment)

        print("Number of quality segments is %d" % len(quality_segments))

        last_segment = quality_segments[0]
        for curr_segment in quality_segments[1:]:
            print("Considering segments %s and %s" % (last_segment, curr_segment))

            if len(last_segment) == 0:
                # If the last segment has no points, we can't compare last and
                # current, but should reset last, otherwise, we will be stuck
                # forever
                logging.info("len(last_segment) = %d, len(curr_segment) = %d, skipping" %
                    (len(last_segment), len(curr_segment)))
                last_segment = curr_segment
                continue

            if len(curr_segment) == 0:
                # If the current segment has no points, we can't compare last and
                # current, but can just continue since the for loop will reset current
                logging.info("len(last_segment) = %d, len(curr_segment) = %d, skipping" %
                    (len(last_segment), len(curr_segment)))
                continue
            get_coords = lambda i: [with_speeds_df.iloc[i]["mLongitude"], with_speeds_df.iloc[i]["mLatitude"]]
            get_ts = lambda i: with_speeds_df.iloc[i]["mTime"]
            # I don't know why they would use time instead of distance, but
            # this is what the existing POSDAP code does.
            print("About to compare curr_segment duration %s with last segment duration %s" %
                            (get_ts(curr_segment[-1]) - get_ts(curr_segment[0]),
                             get_ts(last_segment[-1]) - get_ts(last_segment[0])))
            if (get_ts(curr_segment[-1]) - get_ts(curr_segment[0]) <=
                get_ts(last_segment[-1]) - get_ts(last_segment[0])):
                print("curr segment %s is shorter, cut it" % curr_segment)
                ref_idx = last_segment[-1]
                for curr_idx in curr_segment:
                    print("Comparing distance %s with speed %s * time %s = %s" %
                        (math.fabs(ec.calDistance(get_coords(ref_idx), get_coords(curr_idx))),
                         old_div(self.maxSpeed, 100), abs(get_ts(ref_idx) - get_ts(curr_idx)),
                         self.maxSpeed / 100 * abs(get_ts(ref_idx) - get_ts(curr_idx))))

                    if (math.fabs(ec.calDistance(get_coords(ref_idx), get_coords(curr_idx))) >
                        (self.maxSpeed / 1000 * abs(get_ts(ref_idx) - get_ts(curr_idx)))):
                        print("Distance is greater than max speed * time, deleting %s" % curr_idx)
                        self.inlier_mask_[curr_idx] = False
            else:
                print("prev segment %s is shorter, cut it" % last_segment)
                ref_idx = curr_segment[-1]
                for curr_idx in reversed(last_segment):
                    print("Comparing distance %s with speed %s * time %s = %s" %
                        (math.fabs(ec.calDistance(get_coords(ref_idx), get_coords(curr_idx))),
                         old_div(self.maxSpeed, 1000) , abs(get_ts(ref_idx) - get_ts(curr_idx)),
                         self.maxSpeed / 1000 * abs(get_ts(ref_idx) - get_ts(curr_idx))))
                    if (abs(ec.calDistance(get_coords(ref_idx), get_coords(curr_idx))) >
                        (self.maxSpeed / 1000 *  abs(get_ts(ref_idx) - get_ts(curr_idx)))):
                        print("Distance is greater than max speed * time, deleting %s" % curr_idx)
                        self.inlier_mask_[curr_idx] = False
            last_segment = curr_segment
        logging.info("Filtering complete, removed indices = %s" % np.nonzero(self.inlier_mask_))

class SmoothPiecewiseRansac(object):
    def __init__(self, maxSpeed = 100):
        self.maxSpeed = maxSpeed

    def filter_area_using_ransac(self, area_df):
        from sklearn import linear_model
        import numpy as np
        latArr = [[lat] for lat in area_df.mLatitude.to_numpy()]
        lngArr = area_df.mLongitude.to_numpy()
        model_ransac = linear_model.RANSACRegressor(linear_model.LinearRegression())
        model_ransac.fit(latArr, lngArr)
        inlier_mask = model_ransac.inlier_mask_
        logging.debug("In area %s - %s, deleted %d points through ransac filtering" %
            (area_df.index[0], area_df.index[-1], np.count_nonzero(np.logical_not(inlier_mask))))
        return inlier_mask 

    def find_areas_of_interest(self, with_speeds_df):
        candidateIndices = np.nonzero(with_speeds_df.speed > self.maxSpeed)[0]
        logging.debug("Found %d potential outliers, list = %s" % (len(candidateIndices), candidateIndices))
        if len(candidateIndices) == 0:
            logging.info("No potential outliers (%s), so no areas to consider", candidateIndices)
            return []
        if len(candidateIndices) == 1:
            candidateClusterCenters = [candidateIndices]
            logging.debug("Only one candidate, cluster centers are %s" % candidateClusterCenters)
        else:
            from sklearn.cluster import AffinityPropagation
            af = AffinityPropagation().fit([[i] for i in candidateIndices])
            candidateClusterCenters = af.cluster_centers_
            logging.debug("Found %d clusters with centers %s" % (len(candidateClusterCenters), candidateClusterCenters))
        dfList = []
        for cc in candidateClusterCenters:
            logging.debug("Considering candidate cluster center %s" % cc)
            lowRange = max(cc[0]-5,0)
            highRange = min(cc[0]+5,with_speeds_df.shape[0])
            logging.debug("lowRange = max(%s, %s) = %s and highRange = max(%s, %s) = %s" % (cc[0]-5,0,lowRange,cc[0]+5,with_speeds_df.shape[0],highRange))
            dfList.append(with_speeds_df.loc[lowRange:highRange])
        return dfList

    def filter(self, with_speeds_df):
        ransac_mask = pd.Series([True] * with_speeds_df.shape[0])
        areas_of_interest = self.find_areas_of_interest(with_speeds_df)
        for area in areas_of_interest:
            logging.debug("Area size = %s, index = %s with size %s" % (area.shape[0], area.index, len(area.index)))
            retain_mask = self.filter_area_using_ransac(area)
            logging.debug("Retain mask is of size %d" % len(retain_mask))
            ransac_mask[area.index] = retain_mask
        logging.debug("with speed df shape is %s, ransac_mask size = %s" % (with_speeds_df.shape, len(ransac_mask)))
        logging.debug("filtering done, ransac deleted points = %s" % np.nonzero(ransac_mask == False))
        self.inlier_mask_ = ransac_mask.to_numpy().tolist()
