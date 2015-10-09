# Standard imports
import logging

import numpy as np

# Our imports
import emission.analysis.intake.segmentation.section_segmentation as eaiss
import emission.core.wrapper.motionactivity as ecwm
import emission.core.wrapper.location as ecwl

class SmoothedHighConfidenceMotion(eaiss.SectionSegmentationMethod):
    """
    Determines segmentation points within a trip. It does this by looking at
    the activities detected on the phone with a confidence > the specified
    threshold, and creating a new section for each transition. It also supports
    filtering out certain transitions, such as TILTING or UNKNOWN.
    """

    def __init__(self, confidence_threshold, ignore_modes_list):
        self.confidence_threshold = confidence_threshold
        self.ignore_modes_list = ignore_modes_list

    def is_filtered(self, curr_activity_doc):
        curr_activity = ecwm.Motionactivity(curr_activity_doc)
        logging.debug("curr activity = %s" % curr_activity)
        if (curr_activity.confidence > self.confidence_threshold and
                    curr_activity.type not in self.ignore_modes_list):
            return True
        else:
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
            logging.warning("Found filter_mask with shape (0,0), returning blank")
            return []

        logging.debug("filtered points %s" % np.nonzero(filter_mask))
        logging.debug("motion_df = %s" % motion_df.head())
        filtered_df = motion_df[filter_mask]

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
            if curr_motion.type != curr_start_motion.type:
                # Because the curr_start_motion is initialized with the first
                # motion.  So when idx == 0, the activities will be equal and
                # this is guaranteed to not be invoked
                assert (idx > 0)
                logging.debug("At %s, found new activity %s compared to current %s - creating new section with start_time %s" %
                      (curr_motion.fmt_time, curr_motion.type, curr_start_motion.type,
                       curr_motion.fmt_time))
                # complete this section
                motion_change_list.append((curr_start_motion, curr_motion))
                curr_start_motion = curr_motion
            else:
                logging.debug("At %s, retained existing activity %s because of no change" %
                      (curr_motion.fmt_time, curr_motion.type))
            prev_motion = curr_motion

        logging.info("Detected trip end! Ending section at %s" % curr_motion.fmt_time)
        motion_change_list.append((curr_start_motion, curr_motion))

        # Go from activities to
        # Merge short sections
        # Sometimes, the sections flip-flop around 
        return motion_change_list

    def segment_into_sections(self, timeseries, time_query):
        """
        Determine locations within the specified time that represent segmentation points for a trip.
        :param timeseries: the time series for this user
        :param time_query: the range to consider for segmentation
        :return: a list of tuples [(start1, end1), (start2, end2), ...] that represent the start and end of sections
        in this time range. end[n] and start[n+1] are typically assumed to be adjacent.
        """
        motion_changes = self.segment_into_motion_changes(timeseries, time_query)
        location_points = timeseries.get_data_df("background/filtered_location", time_query)

        # Create sections for each motion. At this point, we need to decide a policy on how to deal with the gaps.
        # Let's pick a reasonable default for now.
        # TODO: Restructure into policy that can be passed in.
        section_list = []
        for (start_motion, end_motion) in motion_changes:
            logging.debug("Considering %s from %s -> %s" %
                          (start_motion.type, start_motion.fmt_time, end_motion.fmt_time))
            # Find points that correspond to this section
            raw_section_df = location_points[(location_points.ts >= start_motion.ts) &
                                             (location_points.ts <= end_motion.ts)]
            if len(raw_section_df) == 0:
                logging.warn("Found no location points between %s and %s" % (start_motion, end_motion))
            else:
                logging.debug("with iloc, section start point = %s, section end point = %s" %
                              (ecwl.Location(raw_section_df.iloc[0]), ecwl.Location(raw_section_df.iloc[-1])))
                section_list.append((raw_section_df.iloc[0], raw_section_df.iloc[-1], start_motion.type))
        return section_list
