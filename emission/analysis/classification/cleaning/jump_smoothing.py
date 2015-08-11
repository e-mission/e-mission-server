# Techniques to smooth jumps in location tracking. Each of these returns a
# boolean mask of inliers and outliers. We assume that the incoming dataframe
# has a column called "speed" that represents the speed at each point. The
# speed of the first point is zero.
# The result is in the inlier_mask field of the appropriate object

# Standard imports
import logging
import math
import pandas as pd
import numpy as np
import attrdict as ad

import emission.analysis.point_features as pf
import emission.core.common as ec
logging.basicConfig(level=logging.DEBUG)

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
            get_coords = lambda(i): [with_speeds_df.iloc[i]["mLongitude"], with_speeds_df.iloc[i]["mLatitude"]]
            get_ts = lambda(i): with_speeds_df.iloc[i]["mTime"]
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
                         self.maxSpeed, abs(get_ts(ref_idx) - get_ts(curr_idx)),
                         self.maxSpeed * abs(get_ts(ref_idx) - get_ts(curr_idx))))

                    if (math.fabs(ec.calDistance(get_coords(ref_idx), get_coords(curr_idx))) >
                        (self.maxSpeed * abs(get_ts(ref_idx) - get_ts(curr_idx)))):
                        print("Distance is greater than max speed * time, deleting %s" % curr_idx)
                        self.inlier_mark_[curr_idx] = False
            else:
                print("prev segment %s is shorter, cut it" % last_segment)
                ref_idx = curr_segment[-1]
                for curr_idx in reversed(last_segment):
                    print("Comparing distance %s with speed %s * time %s = %s" %
                        (math.fabs(ec.calDistance(get_coords(ref_idx), get_coords(curr_idx))),
                         self.maxSpeed, abs(get_ts(ref_idx) - get_ts(curr_idx)),
                         self.maxSpeed * abs(get_ts(ref_idx) - get_ts(curr_idx))))
                    if (abs(ec.calDistance(get_coords(ref_idx), get_coords(curr_idx))) >
                        (self.maxSpeed *  abs(get_ts(ref_idx) - get_ts(curr_idx)))):
                        print("Distance is greater than max speed * time, deleting %s" % curr_idx)
                        self.inlier_mark_[curr_idx] = False
            last_segment = curr_segment
        logging.info("Filtering complete, removed indices = %s" % np.nonzero(self.inlier_mask_))

class SmoothPiecewiseRansac(object):
    def __init__(self, maxSpeed = 100):
        self.maxSpeed = maxSpeed

    def filter_area_using_ransac(self, area_df):
        from sklearn import linear_model
        import numpy as np
        latArr = [[lat] for lat in area_df.mLatitude.as_matrix()]
        lngArr = area_df.mLongitude.as_matrix()
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
        logging.debug("filtering done, ransac deleted points = %s" % np.nonzero(ransac_mask.as_matrix))
        self.inlier_mask_ = ransac_mask.as_matrix().tolist()
