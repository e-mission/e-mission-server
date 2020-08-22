from __future__ import unicode_literals
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import
# Standard imports
from future import standard_library
standard_library.install_aliases()
from builtins import *
import logging
import copy

import numpy as np

# Our imports
import emission.analysis.intake.segmentation.section_segmentation as eaiss
import emission.analysis.intake.segmentation.section_segmentation_methods.flip_flop_detection as ffd
import emission.core.wrapper.motionactivity as ecwm
import emission.core.wrapper.location as ecwl
import emission.core.common as ecc

import emission.analysis.config as eac
import emission.analysis.intake.location_utils as eail
import emission.analysis.intake.domain_assumptions as eaid

class SmoothedHighConfidenceMotion(eaiss.SectionSegmentationMethod):
    """
    Determines segmentation points within a trip. It does this by looking at
    the activities detected on the phone with a confidence > the specified
    threshold, and creating a new section for each transition. It also supports
    filtering out certain transitions, such as TILTING or UNKNOWN.
    """

    def __init__(self, confidence_threshold, distance_threshold, ignore_modes_list):
        self.confidence_threshold = confidence_threshold
        self.distance_threshold = distance_threshold
        self.ignore_modes_list = ignore_modes_list

    def is_filtered(self, curr_activity_doc):
        curr_activity = ecwm.Motionactivity(curr_activity_doc)
        activity_dump = ("curr activity = %s" % curr_activity)
        if (curr_activity.confidence > self.confidence_threshold and
                    curr_activity.type not in self.ignore_modes_list):
            logging.debug("%s, returning True" % activity_dump)
            return True
        else:
            logging.debug("%s, returning False" % activity_dump)
            return False

    def segment_into_motion_changes(self, timeseries, time_query):
        """
        Use the motion changes detected on the phone to detect sections (consecutive chains of points)
        that have a consistent motion.
        :param timeseries: the time series for this user
        :param time_query: the range to consider for segmentation
        :return: a list of tuples [(start_motion, end_motion)] that represent the ranges with a consistent motion.
        The gap between end_motion[n] and start_motion[n+1] represents the transition between the activities.
        We don't actually know the motion/activity in that range with any level of confidence. We need a policy on
        how to deal with them (combine with first, combine with second, split in the middle). This policy can be
        enforced when we map the activity changes to locations.
        """
        motion_df = timeseries.get_data_df("background/motion_activity", time_query)
        filter_mask = motion_df.apply(self.is_filtered, axis=1)
        # Calling np.nonzero on the filter_mask even if it was related trips with zero sections
        # has not been a problem before this - the subsequent check on the
        # length of the filtered dataframe was sufficient. But now both Tom and
        # I have hit it (on 18th and 21st of Sept) so let's handle it proactively here.
        if filter_mask.shape == (0,0):
            logging.info("Found filter_mask with shape (0,0), returning blank")
            return []

        # Replace RUNNING with WALKING so that we don't get extra, incorrect sections 
        # https://github.com/e-mission/e-mission-server/issues/577#issuecomment-379496118
        motion_df["type"].replace([8], 7, inplace=True)

        logging.debug("filtered points %s" % np.nonzero(filter_mask.to_numpy()))
        logging.debug("motion_df = %s" % motion_df.head())
        filtered_df = motion_df[filter_mask]
        filtered_df.reset_index(inplace=True)

        if len(filtered_df) == 0:
            # If there were no entries in the filtered_df, then there are no sections,
            # and we need to return an empty list. This check enforces that...
            return []

        motion_change_list = []
        prev_motion = None
        curr_start_motion = ecwm.Motionactivity(filtered_df.iloc[0])
        #         curr_section = ad.AttrDict({"user_id": trip.user_id, "loc_filter": trip.loc_filter,
        #                                     "start_ts": trip.start_ts, "start_time": trip.start_time,
        #                                     "activity": no_tilting_points_df.iloc[0].activity})

        for idx, row in filtered_df.iterrows():
            curr_motion = ecwm.Motionactivity(row)
            curr_motion.update({"idx": idx})
            # Since the start motion is set upstream makes sure to set an idx
            # for it too
            if curr_motion.ts == curr_start_motion.ts:
                curr_start_motion.update({"idx": idx})
            if curr_motion.type != curr_start_motion.type:
                # Because the curr_start_motion is initialized with the first
                # motion.  So when idx == 0, the activities will be equal and
                # this is guaranteed to not be invoked
                assert (idx > 0)
                logging.debug("At idx %d, time %s, found new activity %s compared to current %s" %
                      (idx, curr_motion.fmt_time, curr_motion.type, curr_start_motion.type))
                curr_end_motion = get_curr_end_motion(prev_motion, curr_motion)
                logging.debug("creating new section for %s at %s -> %s with start_time %s -> %s" % (curr_start_motion.type,
                    curr_start_motion["idx"], curr_end_motion["idx"],
                    curr_start_motion.fmt_time, curr_end_motion.fmt_time))
                # complete this section
                motion_change_list.append((curr_start_motion, curr_end_motion))
                curr_start_motion = curr_end_motion
            else:
                logging.debug("At %s, retained existing activity %s because of no change" %
                      (curr_motion.fmt_time, curr_motion.type))
            prev_motion = curr_motion

        logging.info("Detected trip end! Ending section at %s" % curr_motion.fmt_time)
        motion_change_list.append((curr_start_motion, curr_motion))

        smoothed_motion_list = ffd.FlipFlopDetection(motion_change_list, self).merge_flip_flop_sections()
        return smoothed_motion_list

    # Overridden in smoothed_high_confidence_with_visit_transitions.py.
    # Consider porting any changes there as well if applicable.
    def segment_into_sections(self, timeseries, distance_from_place, time_query):
        """
        Determine locations within the specified time that represent segmentation points for a trip.
        :param timeseries: the time series for this user
        :param time_query: the range to consider for segmentation
        :return: a list of tuples [(start1, end1), (start2, end2), ...] that represent the start and end of sections
        in this time range. end[n] and start[n+1] are typically assumed to be adjacent.
        """
        self.get_location_streams_for_trip(timeseries, time_query)
        motion_changes = self.segment_into_motion_changes(timeseries, time_query)

        if len(self.location_points) == 0:
            logging.debug("No location points found for query %s, returning []" % time_query)
            return []

        fp = self.location_points.iloc[0]
        lp = self.location_points.iloc[-1]

        # Create sections for each motion. At this point, we need to decide a policy on how to deal with the gaps.
        # Let's pick a reasonable default for now.
        # TODO: Restructure into policy that can be passed in.
        section_list = []
        for i, (start_motion, end_motion) in enumerate(motion_changes):
            logging.debug("Considering %s from %s -> %s" %
                          (start_motion.type, start_motion.fmt_time, end_motion.fmt_time))
            # Find points that correspond to this section
            raw_section_df = self.location_points[(self.location_points.ts >= start_motion.ts) &
                                             (self.location_points.ts <= end_motion.ts)]
            if len(raw_section_df) == 0:
                logging.info("Found no location points between %s and %s" % (start_motion, end_motion))
            else:
                logging.debug("with iloc, section start point = %s, section end point = %s" %
                              (ecwl.Location(raw_section_df.iloc[0]), ecwl.Location(raw_section_df.iloc[-1])))
                section_list.append((raw_section_df.iloc[0], raw_section_df.iloc[-1], start_motion.type))

        logging.debug("len(section_list) == %s" % len(section_list))
        if len(section_list) == 0:
            if len(motion_changes) == 1:
                (start_motion, end_motion) = motion_changes[0]

                if start_motion.type == end_motion.type:
                    logging.debug("No section because start_motion == end_motion, creating one dummy section")
                    section_list.append((fp, lp, start_motion.type))

            self.squish_stops(motion_changes, section_list)

            if len(motion_changes) == 0:
            # there are no high confidence motions useful motions, so we add a section of type NONE
            # as long as it is a discernable trip (end != start) and not a spurious trip
                if distance_from_place > self.distance_threshold:
                    logging.debug("No high confidence motions, but "
                        "distance %s > threshold %s, creating dummy section of type UNKNOWN" %
                                  (distance_from_place, self.distance_threshold))
                    section_list.append((fp, lp, ecwm.MotionTypes.UNKNOWN))

        return section_list


    def get_location_streams_for_trip(self, timeseries, time_query):
        # Let's also read the unfiltered locations so that we can combine them with 
        # the sampled locations
        self.unfiltered_loc_df = timeseries.get_data_df("background/location", time_query)
        self.location_points = timeseries.get_data_df("background/filtered_location", time_query)
        # Location points can have big gaps. Let's extrapolate them so that we
        # can use them better.
        # https://github.com/e-mission/e-mission-server/issues/577#issuecomment-377323407
        self.resampled_loc_df = eail.resample(self.location_points, interval = 10)

    def filter_points_for_range(self, df, start_motion, end_motion):
        """
        Gets the points from the dataframe that are in the range (sm.ts, em.ts)
        """
        return df[(df.ts >= start_motion.ts) &
                  (df.ts <= end_motion.ts)]

    def squish_stops(self, motion_changes, section_list):
        STOP_DISTANCE_THRESHOLD = max(eac.get_config()["section.startStopRadius"],
            eac.get_config()["section.endStopRadius"])

        for i, x in enumerate(zip(section_list, section_list[1:])):
            try:
                ((ss1, se1, st1), (ss2, se2, st2)) = x
            except ValueError as e:
                print(len(x), [len(xm) for xm in x])
                print(x[0])
            # i is the index of s1, i+1 is the index of s2
            stop_duration = ss2.ts - se1.ts
            stop_distance = ecc.calDistance(ss2.loc["loc"]["coordinates"],
                                            se1.loc["loc"]["coordinates"])
            logging.debug("while squishing stop, %s (%d) -> %s (%d), duration = %d, dist = %d" % 
                        (se1.fmt_time, i, ss2.fmt_time, i+1, stop_duration, stop_distance))
            if stop_distance > STOP_DISTANCE_THRESHOLD:
                mcs1, mce1 = motion_changes[i]
                mcs2, mce2 = motion_changes[i+1]
                assert mce1.ts == mcs2.ts, \
                    "curr motion change ends at %s, next motion change ends at %s" % \
                        (mce1.fmt_time, mce2.fmt_time)

                # e.g. if the motion changed at 11:33, mce1.ts = mcs2.ts = 11:33
                # if the first section's last point (se1) was at 11:31 and the
                # second section's first point (ss2) was at 11:51, then we
                # should set the second section's first point to be the first
                # section's last point (e.g. ss2 = se1)

                stop_start_gap = mce1.ts - se1.ts
                stop_end_gap = ss2.ts - mcs2.ts
                gap_ratio = max(stop_start_gap, stop_end_gap)/min(stop_start_gap, stop_end_gap)

                log_msg = ("need to squish, comparing start_gap = %d, end_gap = %d, ratio = %4f" % 
                    (stop_start_gap, stop_end_gap, gap_ratio))
                if gap_ratio >= 1.5:
                    if stop_start_gap < stop_end_gap:
                        logging.debug(log_msg + ", setting ss2 <- se1")
                        section_list[i+1] = (se1, se2, st2)
                    else:
                        logging.debug(log_msg + ", setting se1 <- ss2")
                        section_list[i] = (ss1, ss2, st1)
                else:
                    logging.debug(log_msg + ", no change, fixed in clean_and_resample")

def get_curr_end_motion(prev_motion, curr_motion):
    """
    Get the end motion based on the transition.
    In general, while starting, we want to end at the prev_motion
                while ending, we want to end at the curr_motion
    https://github.com/e-mission/e-mission-server/issues/577#issuecomment-377742975

    There are 4 cases:
    non-motorized -> motorized: prev_motion
    motorized -> non-motorized: curr_motion
    non-motorized -> non-motorized: prev_motion (worked well for the flip-flopping at the end)
    motorized -> motorized: unsure if this ever happens, return curr_motion for now
    """

    prev_motorized = eaid.is_motorized(prev_motion.type)
    curr_motorized = eaid.is_motorized(curr_motion.type)

    if not prev_motorized and curr_motorized:
        return prev2end(prev_motion, curr_motion)
    elif prev_motorized and not curr_motorized:
        return curr_motion
    elif not prev_motorized and not curr_motorized:
        if eaid.is_walking_type(prev_motion.type) and \
            curr_motion.type == ecwm.MotionTypes.BICYCLING:
            # WALK -> BIKE transition, merge backwards
            return prev2end(prev_motion, curr_motion)
        elif prev_motion.type == ecwm.MotionTypes.BICYCLING and \
            eaid.is_walking_type(curr_motion.type):
            return curr_motion
        else:
            # both modes are walking or bicycling, let's just merge forward
            return curr_motion
    else:
        return curr_motion

# Transition at the previous motion by retaining the timestamp but setting the
# end to the type
def prev2end(prev_motion, curr_motion):
    curr_end_motion = copy.copy(prev_motion)
    curr_end_motion["type"] = curr_motion.type
    curr_end_motion["confidence"] = curr_motion.confidence
    return curr_end_motion
