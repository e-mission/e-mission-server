from __future__ import unicode_literals
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import
# Standard imports
from future import standard_library
standard_library.install_aliases()
from builtins import *
import logging

# Our imports
import emission.analysis.intake.segmentation.section_segmentation_methods.smoothed_high_confidence_motion as eaisms
import emission.core.wrapper.motionactivity as ecwm
import emission.core.wrapper.location as ecwl
import emission.analysis.intake.segmentation.section_segmentation_methods.reconstruct_locations as eaissr

class SmoothedHighConfidenceMotionWithVisitTransitions(eaisms.SmoothedHighConfidenceMotion):
    def create_unknown_section(self, location_points_df):
        assert(len(location_points_df) > 0)
        return (location_points_df.iloc[0], location_points_df.iloc[-1], ecwm.MotionTypes.UNKNOWN)

    def get_section_if_applicable(self, timeseries, distance_from_start, time_query, location_points):
        # We don't have any motion changes. So let's check to see if we
        # have a visit transition, which will help us distinguish between
        # real and fake trips.

        # Yech! This feels really hacky, but if we have a really short trip,
        # then we may get the visit ending message after the trip has ended.
        # So let's expand the time query by 5 minutes.
        # This is based on the 10:06 -> 10:07 trip from the 22 Feb test case
        time_query.endTs = time_query.endTs + 5 * 60
        transition_df = timeseries.get_data_df('statemachine/transition', time_query)
        if len(transition_df) == 0:
            logging.debug("there are no transitions, which means no visit transitions, not creating a section")
            return None

        if distance_from_start > self.distance_threshold:
            logging.debug("found distance %s > threshold %s, returning dummy section" %
                          (distance_from_start, self.distance_threshold))
            return self.create_unknown_section(self.location_points)

        visit_ended_transition_df = transition_df[transition_df.transition == 14]
        if len(visit_ended_transition_df) == 0:
            logging.debug("there are some transitions, but none of them are visit, not creating a section")
            return None

        # We have a visit transition, so we have a pretty good idea that
        # this is a real section. So let's create a dummy section for it and return
        logging.debug("found visit transition %s, returning dummy section" % visit_ended_transition_df[["transition", "fmt_time"]])
        return self.create_unknown_section(self.location_points)

    def extend_activity_to_location(self, motion_change, location_point):
        new_mc = ecwm.Motionactivity({
            'type': motion_change.type,
            'confidence': motion_change.confidence,
            'ts': location_point.data.ts,
            'local_dt': location_point.data.local_dt,
            'fmt_time': location_point.data.fmt_time
        })
        return new_mc

    def segment_into_sections(self, timeseries, distance_from_place, time_query):
        """
        Determine locations within the specified time that represent segmentation points for a trip.
        :param timeseries: the time series for this user
        :param time_query: the range to consider for segmentation
        :return: a list of tuples [(start1, end1), (start2, end2), ...] that represent the start and end of sections
        in this time range. end[n] and start[n+1] are typically assumed to be adjacent.
        """

        # Since we are going to use a hybrid model, let's just read all kinds
        # of locations upfront
        self.get_location_streams_for_trip(timeseries, time_query)
        motion_changes = self.segment_into_motion_changes(timeseries, time_query)

        if len(self.location_points) == 0:
            logging.debug("There are no points in the trip. How the heck did we segment it?")
            return []

        if len(motion_changes) == 0:
            dummy_sec = self.get_section_if_applicable(timeseries, distance_from_place,
                                                       time_query, self.location_points)
            if dummy_sec is not None:
                return [dummy_sec]
            else:
                return []

        # Now, we know that we have location points and we have motion_changes.
        section_list = []
        # Sometimes, on iOS, we have no overlap between motion detection
        # and location points.
        # In a concrete example, the motion points are:
        # 13         100             high    10  2016-02-22T15:36:06.491621-08:00
        # 14         100             high     0  2016-02-22T15:36:09.353743-08:00
        # 15         100             high    10  2016-02-22T15:36:13.169997-08:00
        # 16          75           medium     0  2016-02-22T15:36:13.805993-08:00
        # while the trip points are 2016-02-22T15:36:00 and then
        # 2016-02-22T15:36:23. So there are no location points within
        # that very narrow range. And there are no more motion points
        # until the trip end at 15:37:35. This is because, unlike android,
        # we cannot specify a sampling frequency for the motion activity
        # So let us extend the first motion change to the beginning of the
        # trip, and the last motion change to the end of the trip
        motion_changes[0] = (self.extend_activity_to_location(motion_changes[0][0],
                timeseries.df_row_to_entry("background/filtered_location",
                                           self.location_points.iloc[0])),
                             motion_changes[0][1])
        motion_changes[-1] = (motion_changes[-1][0],
            self.extend_activity_to_location(motion_changes[-1][1],
                timeseries.df_row_to_entry("background/filtered_location",
                                           self.location_points.iloc[-1])))

        for (start_motion, end_motion) in motion_changes:
            logging.debug("Considering %s from %s -> %s" %
                          (start_motion.type, start_motion.fmt_time, end_motion.fmt_time))
            # Find points that correspond to this section
            unfiltered_section_df = self.filter_points_for_range(self.unfiltered_loc_df,
                                        start_motion, end_motion)
            raw_section_df = self.filter_points_for_range(self.location_points,
                                        start_motion, end_motion)
            resampled_sec_df = self.filter_points_for_range(self.resampled_loc_df,
                                        start_motion, end_motion)

            if len(raw_section_df) <=1 and len(unfiltered_section_df) <= 1:
                logging.info("Found %d filtered points and %d unfiltered points between %s and %s for type %s, skipping..." % (len(raw_section_df), len(unfiltered_section_df),
                                      start_motion.fmt_time, end_motion.fmt_time,
                                      start_motion.type))
            else:
                section_start_loc = eaissr.get_matched_location(start_motion, raw_section_df, resampled_sec_df, 0, timeseries)
                section_end_loc = eaissr.get_matched_location(end_motion, raw_section_df, resampled_sec_df, -1, timeseries)
                logging.debug("section start point = %s, section end point = %s" %
                              (ecwl.Location(section_start_loc), ecwl.Location(section_end_loc)))
                section_list.append((section_start_loc, section_end_loc, start_motion.type))
            # if this lack of overlap is part of an existing set of sections,
            # then it is fine, because in the section segmentation code, we
            # will mark it as a transition
        return section_list

