import logging
import itertools
import enum

import emission.core.wrapper.motionactivity as ecwm
import emission.core.wrapper.location as ecwl
import emission.core.common as ecc

import emission.analysis.intake.cleaning.location_smoothing as eaicl
import emission.analysis.intake.domain_assumptions as eaid
import emission.analysis.config as eac

@enum.unique
class Direction(enum.Enum):
    FORWARD = 1
    BACKWARD = -1
    NONE = 0
    NEW = 2

@enum.unique
class FinalMode(enum.Enum):
    NA = 0
    UNMERGED = 1
    MERGED = -1
    NEW = 2

class MergeResult:
    def __init__(self, direction, final_mode, new_mode=None):
        self.direction = direction
        self.final_mode = final_mode
        self.new_mode = new_mode

    def __str__(self):
        return "(%s, %s)" % (self.direction, self.final_mode)

    @staticmethod
    def NONE():
        return MergeResult(Direction.NONE, FinalMode.NA)

class MergeAction:
    def __init__(self, start, end, final_mode):
        self.start = start
        self.end = end
        self.final_mode = final_mode

class FlipFlopDetection():
    def __init__(self, motion_changes, seg_method):
        # List of motion activity objects representing the start and end of sections
        self.motion_changes = motion_changes
        self.seg_method = seg_method
        self.add_trip_percentages()

    def add_trip_percentages(self):
        if len(self.motion_changes) > 0:
            total_trip_time = self.motion_changes[-1][1].ts - self.motion_changes[0][0].ts

            for sm, em in self.motion_changes:
                curr_section_time = em.ts - sm.ts
                sm.update({"trip_pct": (curr_section_time * 100)/total_trip_time})
        else:
            logging.info("No motion changes, skipping trip percentages...")

    def is_flip_flop(self, start_motion, end_motion):
        """
        Current definition for when there is a potential flip flop
        - if the transition was based on 1 motion activity 
        """
        if start_motion["trip_pct"] > 25:
            logging.debug("trip_pct = %d, > 25, returning False" % start_motion["trip_pct"])
            return False
        idx_diff = end_motion["idx"] - start_motion["idx"]
        if idx_diff <= 1:
            logging.debug("in is_flip_flop: idx_diff = %d" % idx_diff)
            return True
        if not eaid.is_walking_type(start_motion.type) and idx_diff <= 2:
            # for bicycling and transport, we want idx = 2
            # https://github.com/e-mission/e-mission-server/issues/577#issuecomment-379527711
            logging.debug("in non-walking is_flip_flop: idx_diff = %d" % idx_diff)
            return True
        elif not self.is_valid_for_type((start_motion, end_motion)):
            logging.debug("in is_flip_flop: is_valid_for_type is false")
            return True
        else:
            streak_locs = self.seg_method.filter_points_for_range(
                self.seg_method.location_points, start_motion, end_motion)
            logging.debug("in is_flip_flop: len(streak_locs) = %d" % len(streak_locs))
            if len(streak_locs) == 0:
                return True
        return False

    def should_merge(self, streak_start, streak_end):
        """
        Current checks for whether we should merge or not:
        - can we erase the flip
        
        returns: {"merge": , "mode": }
               valid merge values:
               +1 to merge forwards (e.g. expand the existing previous section forwards)
               -1 to merge backwards (e.g. expand the existing next section backwards)
                 0 to not merge
                2 to merge into a new section

                valid set_mode_to values
                - unmerged: final mode of (merged + unmerged) = unmerged (most
                  common)
                - merged: final mode of (merged + unmerged) = merged (for
                  special cases like the bike check)
        """

        logging.debug("Looking to see whether we should merge streak %s -> %s, length %d" %
            (streak_start, streak_end, streak_end - streak_start))
        if (streak_start) == streak_end or \
            (streak_start + 1) == streak_end:
            logging.info("Found single flip-flop %s -> %s -> %s" % 
                (streak_start - 1, streak_start, streak_start+1))
            cfbs  = self.check_fast_biker_special(streak_start, streak_end)
            ctv = self.check_transition_validity(streak_start, streak_end)
            cnlw = self.check_no_location_walk(streak_start, streak_end)
            if cfbs.direction != Direction.NONE:
                return cfbs
            if ctv.direction != Direction.NONE:
                return ctv
            if cnlw.direction != Direction.NONE:
                return cnlw
            return MergeResult.NONE()

        # As an easy fix, let us assume that any long flip-flop streak needs to
        # be a new section. And most of the time, it is going to be bicycling,
        # at least according to the data we have seen so far, so let's just set
        # that anyway
        if eaid.is_flip_flop_streak_for_new_section(streak_end - streak_start):
            return MergeResult(Direction.NEW, FinalMode.NEW, ecwm.MotionTypes.BICYCLING)

        cvft = self.check_valid_for_type(streak_start, streak_end)
        if cvft.direction != Direction.NONE:
            return cvft
        
        return MergeResult.NONE()

    def check_fast_biker_special(self, streak_start, streak_end):
        """
        Check a special transition that happens for very fast bikers
        https://github.com/e-mission/e-mission-server/issues/577#issuecomment-378126571
        https://github.com/e-mission/e-mission-server/issues/577#issuecomment-378129015
        """
        start_change = self.motion_changes[streak_start]
        end_change = self.motion_changes[streak_end]
        ssm, sem = start_change
        esm, eem = end_change

        if streak_end + 1 == len(self.motion_changes):
            # There is no after section - cannot be IN_VEHICLE!
            return MergeResult.NONE()

        after_motion = self.motion_changes[streak_end + 1]
        asm, aem = after_motion

        # the idx checks ensure that this was indeed the luck of the draw.
        # if we had chosen the flip the other way then the `IN_VEHICLE` would
        # have been deleted instead of the bicycling
        bike_idx_diff = esm["idx"] - ssm["idx"]
        vehicle_idx_diff = aem["idx"] - asm["idx"]

        transition_interval = esm.ts - sem.ts
        is_short_transition_interval = eaid.is_too_short_bike_vehicle_transition(transition_interval)

        # since we are potentially deleting a section that is NOT a flip flop,
        # add the equivalent of the trip_pct check
        section_ratio = (esm.ts - ssm.ts) / (aem.ts - asm.ts + 1)

        rule_checks_log = (("check_fast_biker_special: "+
            "curr_type = %s, next_type = %s, curr_idx_diff = %d, "+
                "next_idx_diff = %d, short_transition_interval = %s, " +
                "section_ratio = %s") % (ssm.type, asm.type, bike_idx_diff,
            vehicle_idx_diff, is_short_transition_interval, section_ratio))

        if (ssm.type == ecwm.MotionTypes.BICYCLING and 
            asm.type == ecwm.MotionTypes.IN_VEHICLE and
            bike_idx_diff <= 1 and 
            vehicle_idx_diff <=2 and
            is_short_transition_interval and
            (bike_idx_diff == 0 or section_ratio > 0.75)):
                logging.debug(rule_checks_log + ", merged")
                return MergeResult(Direction.BACKWARD, FinalMode.MERGED)

        logging.debug(rule_checks_log + ", unmerged")
        return MergeResult.NONE()

    def check_transition_validity(self, streak_start, streak_end):
        """
        If there is a single flip flop (e.g. WALKING -> BICYCLING -> WALKING),
        the only decision we have is whether to remove the middle transition.
        And we can include a pretty simple fix for that - the only valid
        intermediate transition is WALKING. For all other modes, we expect that
        the travel will be long enough that we will get at least a couple of
        activity points. It's not worth it otherwise
        """
        if not ((streak_start == streak_end) or (streak_start + 1 == streak_end)):
            logging.error("1 flip check called with streak %d -> %d" % (streak_start, streak_end))
            if eac.get_config()["intake.segmentation.section_segmentation.sectionValidityAssertions"]:
                assert False

        start_change = self.motion_changes[streak_start]
        if not eaid.is_walking_type(start_change[0].type):
            logging.debug("single transition %s, not WALKING, merging" % start_change[0].type)
            return self.get_merge_direction(streak_start, streak_end)
        else:
            logging.debug("single transition %s, WALKING, not merging yet" % start_change[0].type)
        return MergeResult.NONE()

    def check_no_location_walk(self, streak_start, streak_end):
        if not ((streak_start == streak_end) or (streak_start + 1 == streak_end)):
            logging.error("1 flip check called with streak %d -> %d" % (streak_start, streak_end))
            if eac.get_config()["intake.segmentation.section_segmentation.sectionValidityAssertions"]:
                assert False

        ssm, sem = self.motion_changes[streak_start]
        streak_locs = self.seg_method.filter_points_for_range(
            self.seg_method.location_points, ssm, sem)
        streak_unfiltered_locs = self.seg_method.filter_points_for_range(
            self.seg_method.unfiltered_loc_df, ssm, sem)

        if len(streak_locs) <= 1:
            # we have no points, not even unfiltered. This must be bogus 
            return self.get_merge_direction(streak_start, streak_end)

        return MergeResult.NONE()

    def check_valid_for_type(self, streak_start, streak_end):
        valid_for_type = True
        for si in range(streak_start, streak_end+1):
            mc = self.motion_changes[si]
            valid_for_type = valid_for_type and self.is_valid_for_type(mc)
        if not valid_for_type:
            # we know that the entire flip-flop is actually one of the two
            # sides. How do we know which side? Compare the speed profile of
            # the section to the speed profiles of the two sides
            # TODO: generalize to longer sequence of flip-flops later
            return self.get_merge_direction(streak_start, streak_end)
        return MergeResult.NONE()


    def get_merge_direction(self, streak_start, streak_end):
        """
        Checks to decide merge direction
        - if either direction is WALKING and speed is greater than 1.4 + slosh then 
            must be the other direction
        - pick direction that is closer to the median speed
        """
        start_change = self.motion_changes[streak_start]
        end_change = self.motion_changes[streak_end]
        ssm, sem = start_change
        esm, eem = end_change

        if streak_start == 0:
            # There is no before section - only choices are to merge backward
            # or make a new section
            logging.debug("get_merge_direction: at beginning of changes, can only merge backward")
            return MergeResult(Direction.BACKWARD, FinalMode.UNMERGED)

        before_motion = self.motion_changes[streak_start - 1]
        bsm, bem = before_motion

        if streak_end + 1 == len(self.motion_changes):
            # There is no after section - only one way to merge!
            logging.debug("get_merge_direction: at end of changes, can only merge forward")
            return MergeResult(Direction.FORWARD, FinalMode.UNMERGED)

        after_motion = self.motion_changes[streak_end + 1]
        asm, aem = after_motion

        if bsm.type == asm.type:
            logging.debug("before type = %s, after type = %s, merge direction is don't care, returning forward"  % 
            (bsm.type, asm.type))
            return MergeResult(Direction.FORWARD, FinalMode.UNMERGED)

        loc_points = self.seg_method.filter_points_for_range(
                self.seg_method.location_points, ssm, eem)
        loc_points.reset_index(inplace=True)
        with_speed_loc_points = eaicl.add_dist_heading_speed(loc_points)

        points_before = self.seg_method.filter_points_for_range(
                self.seg_method.location_points, bsm, bem)
        points_before.reset_index(inplace=True)
        with_speed_points_before = eaicl.add_dist_heading_speed(points_before)

        points_after = self.seg_method.filter_points_for_range(
                self.seg_method.location_points, asm, aem)
        points_after.reset_index(inplace=True)
        with_speed_points_after = eaicl.add_dist_heading_speed(points_after)

        curr_median_speed = self.get_section_speed(loc_points, with_speed_loc_points,
            points_before, points_after)

        # check for walking speed, which is the one constant is a cruel,
        # shifting world where there is no truth
        if (eaid.is_walking_type(asm.type) and 
            (not eaid.is_walking_speed(curr_median_speed))):
            logging.debug("after is walking, but speed is %d, merge forward, returning 1" % curr_median_speed)
            return MergeResult(Direction.FORWARD, FinalMode.UNMERGED)
        elif (eaid.is_walking_type(bsm.type) and 
            (not eaid.is_walking_speed(curr_median_speed))):
            logging.debug("before is walking, but speed is %d, merge backward, returning -1")
            return MergeResult(Direction.BACKWARD, FinalMode.UNMERGED)
            
        logging.debug("while merging, comparing curr speed %s with before %s and after %s" % 
            (curr_median_speed, with_speed_points_before.speed.median(),
            with_speed_points_after.speed.median()))
        if (abs(curr_median_speed - with_speed_points_before.speed.median()) <
            abs(curr_median_speed - with_speed_points_after.speed.median())):
            # speed is closer to before than after, merge with before, merge forward
            logging.debug("before is closer, merge forward, returning 1")
            return MergeResult(Direction.FORWARD, FinalMode.UNMERGED)
        else:
            logging.debug("after is closer, merge backward, returning -1")
            return MergeResult(Direction.BACKWARD, FinalMode.UNMERGED)

    def get_section_speed(self, loc_points, with_speed_loc_points, points_before, points_after):
        if len(loc_points) >1:
            curr_median_speed = with_speed_loc_points.speed.median()
            logging.debug("Median calculation from speeds = %s" % curr_median_speed)
        elif (len(points_before) > 0) and (len(points_after) > 0):
            # We don't have any points of our own. Let's use the last point
            # from before and the first point from after
            last_prev_point = points_before.iloc[-1]
            first_next_point = points_after.iloc[0]
            dist = ecc.calDistance(last_prev_point["loc"]['coordinates'],
                                    first_next_point["loc"]['coordinates'])
            time = first_next_point.ts - last_prev_point.ts
            logging.debug("Backup calculation from %s -> %s. dist = %d, time = %4f, speed = %4f" % 
                (last_prev_point.fmt_time, first_next_point.fmt_time,
                 dist, time, dist/time))
            curr_median_speed = dist/time
        else:
            curr_median_speed = 0
        return curr_median_speed

    def is_valid_for_type(self, motion_change):
        """
        Basic sanity checks for the various types of movement
        Gah this is pathetic, but I am desperate here.
        """
        logging.debug("is_valid_for_type called")
        mcs, mce = motion_change
        validity_check_map = {
            ecwm.MotionTypes.WALKING: self.validate_walking,
            ecwm.MotionTypes.ON_FOOT: self.validate_walking,
            ecwm.MotionTypes.RUNNING: self.validate_walking,
            ecwm.MotionTypes.BICYCLING: self.validate_bicycling,
            ecwm.MotionTypes.IN_VEHICLE: self.validate_motorized
        }

        ret_val = validity_check_map[mcs.type](mcs, mce)
        logging.debug("Sanity checking section %s -> %s for type %s = %s" % 
            (mcs.fmt_time, mce.fmt_time, mcs.type, ret_val))
        return ret_val

    def validate_walking(self, mcs, mce):
        median_speed = self.get_median_speed(mcs, mce)
        if median_speed is not None and not eaid.is_walking_speed(median_speed):
            logging.debug("in validate_walking, median speed = %d, failed" % median_speed)
            return False
        return True

    def validate_bicycling(self, mcs, mce):
        # time shortness check. unlikely to ride a bike for less than a minute
        # and then switch to another mode
        if eaid.is_too_short_bicycle_ride(mce.ts - mcs.ts):
            logging.debug("bike ride length = %d, failed" % (mce.ts - mcs.ts))
            return False
        
        # speed check
        median_speed = self.get_median_speed(mcs, mce)
        if median_speed is not None and not eaid.is_bicycling_speed(median_speed):
            logging.debug("in validate_bicycling, median speed = %d, failed" % median_speed)
            return False
        return True

    def validate_motorized(self, mcs, mce):
        # time shortness check. unlikely to use a motorized mode for less than 5 minutes
        # and then switch to another mode
        if eaid.is_too_short_motorized_ride(mce.ts - mcs.ts):
            return False
        return True

    def get_median_speed(self, mcs, mce):
        loc_df = self.seg_method.filter_points_for_range(
                self.seg_method.location_points, mcs, mce)
        if len(loc_df) > 0:
            loc_df.reset_index(inplace=True)
            with_speed_df = eaicl.add_dist_heading_speed(loc_df)
            return with_speed_df.speed.median()
        else:
            return None

    def get_streaks(self, flip_flop_list):
        """
        :param flip_flop_list: contains a mixture of true and false depending
            on whether it is flip flopping or not.

        :return (start_index, end_index) of each True streak
        [False, False, False, True, True, False, False, False] will return (3,4)
        """
        streak_list = []
        curr_streak_start = 0
        for i, is_ff in enumerate(flip_flop_list):
            if not is_ff:
                if i != curr_streak_start:
                    streak_list.append((curr_streak_start, i-1))
                curr_streak_start = i+1

        if i != (curr_streak_start - 1):
            # There is a streak ending at the end of the list
            streak_list.append((curr_streak_start, len(flip_flop_list) - 2))
        return streak_list

    def merge_streaks_pass_1(self, unmerged_change_list, forward_merged_streaks,
                                   backward_merged_streaks, new_merged_streaks):
        """
        Extends the un-flipflopped sections to cover the flip-flop sections
        that we need to merge. If we have unmerged_change_list = 
            [ 0 (a, b),
              1 (b, c),
              2 (c, d),
              3 (d, e), 
              4 (e, f),
              5 (f, g),
              6 (g, h),
              7 (h, i)]
        forward_merged_streaks = [(1,2), (6, 6)]
        backward_merged_streaks = [(4, 4)]

        we should return [(a, d), (d, e), (e, h), (h, i)]
        """

        modifiable_changes = [[sm, em] for sm, em in unmerged_change_list]

        # The real challenge here is that I want to merge entries in the list
        # while preserving indexed operation. Note that the streak lists are
        # still indexed by the index
        
        # One fix would be to retain the values, but have them be pointers to
        # each other. This would allow multiple modifications (e.g. for a
        # backward merge followed by a forward merge or vice versa)

        # for that to work, though, we need to be able to modify entries, so we
        # need to use lists instead of tuples. And if unique doesn't work, we
        # need to keep track of which are the valid entries

        logging.debug("before merging entries, changes were %s" %
            ([(mc[0]["idx"], mc[1]["idx"]) for mc in modifiable_changes]))

        for ms in forward_merged_streaks:
            # by setting the end motion
            modifiable_changes[ms.start-1][1] = modifiable_changes[ms.end][1]
            # by default, final mode will the mode of (ms.start-1) which is the
            # unmerged mode
            if ms.final_mode == FinalMode.MERGED:
                modifiable_changes[ms.start-1][0]["type"] = \
                    modifiable_changes[ms.start][0]["type"]
            # change all the merged entries to point to the retained entry
            for i in range(ms.start, ms.end+1):
                modifiable_changes[i] = modifiable_changes[ms.start-1]

        for ms in backward_merged_streaks:
            # extend the post-merge section to start with the merged section
            # by setting the start motion
            # in this case, because the start motion determines the section type
            # and we are changing the start section, we should ensure that the 
            # after section's type is the one that is retained
            # https://github.com/e-mission/e-mission-server/issues/577#issuecomment-377863642

            # By default, final mode will be the mode of ms.start, which is the
            # merged mode
            if ms.final_mode == FinalMode.UNMERGED:
                modifiable_changes[ms.start][0]["type"] = \
                    modifiable_changes[ms.end+1][0]["type"]
            modifiable_changes[ms.end+1][0] = modifiable_changes[ms.start][0]
            for i in range(ms.start, ms.end+1):
                modifiable_changes[i] = modifiable_changes[ms.end+1]

        for ms in new_merged_streaks:
            new_mc = (modifiable_changes[ms.start][0], modifiable_changes[ms.end][1])
            new_mc[0]["type"] = ms.final_mode
            for i in range(ms.start, ms.end+1):
                modifiable_changes[i] = new_mc

        logging.debug("before merging entries, changes were %s" %
            ([(mc[0]["idx"], mc[1]["idx"]) for mc in modifiable_changes]))

        ret_list = []
        for mc in modifiable_changes:
            if mc not in ret_list:
                ret_list.append(mc)

        tuple_ret_list = [(mc[0], mc[1]) for mc in ret_list]

        logging.debug("after generating unique entries, list = %s" %
            ([(mc[0]["idx"], mc[1]["idx"]) for mc in tuple_ret_list]))
        return tuple_ret_list

    def merge_streaks_pass_2(self, unmerged_change_list):
        """
        After removing all the flip-flopped sections, merge consecutive
        sections with the same type. So if we have inputs
            [ 0 (a, d, WALKING),
              1 (d, e, VEHICLE),
              2 (e, h, WALKING), 
              3 (h, j, WALKING)]
              4 (k, l, WALKING)]

        we should return [(a, d), (d, e), (e, j), (k, l)]
        """
        merged_list = []

        logging.debug("Before merging, list = %s" %
            ([(mc[0]["type"], mc[1]["type"]) for mc in unmerged_change_list]))
        curr_start_motion = unmerged_change_list[0][0]
        prev_end_motion = None
        for sm, em in unmerged_change_list:
            if sm.type != curr_start_motion.type:
                merged_list.append((curr_start_motion, prev_end_motion))
                curr_start_motion = sm
            prev_end_motion = em

        merged_list.append((curr_start_motion, prev_end_motion))

        logging.debug("After merging, list = %s" %
            ([(mc[0]["type"], mc[1]["type"]) for mc in merged_list]))
        return merged_list

    def is_curr_change_in_merge(self, i, merged_list):
        is_change_list = [(s < i) and (i < e) for s, e in merged_list]
        logging.debug("is_change_list = %s" % is_change_list)
        return reduce(lambda x,y: x and y, is_change_list)


    def merge_flip_flop_sections(self):
        logging.debug("while starting flip_flop detection, changes are %s" %
            ([(mc[0]["idx"], mc[1]["idx"], mc[0]["type"]) for mc in self.motion_changes]))
        self.flip_flop_list = []
        for i, (sm, em) in enumerate(self.motion_changes):
            logging.debug("comparing %s, %s to see if there is a flipflop" %
                (sm["idx"], em["idx"]))
            self.flip_flop_list.append(self.is_flip_flop(sm, em))

        logging.debug("flip_flop_list = %s" % [i for i, ff in enumerate(self.flip_flop_list) if ff])
        self.flip_flop_streaks = self.get_streaks(self.flip_flop_list)
        logging.debug("flip_flop_streaks = %s" % self.flip_flop_streaks)

        forward_merged_streaks = []
        backward_merged_streaks = []
        new_merged_streaks = []
        for streak in self.flip_flop_streaks:
            ss, se = streak
            sm = self.should_merge(ss, se)
            if sm.direction == Direction.FORWARD:
                forward_merged_streaks.append(MergeAction(ss, se, sm.final_mode))
            elif sm.direction == Direction.BACKWARD:
                backward_merged_streaks.append(MergeAction(ss, se, sm.final_mode))
            elif sm.direction == Direction.NEW:
                new_merged_streaks.append(MergeAction(ss, se, sm.new_mode))

        logging.debug("forward merged_streaks = %s" % forward_merged_streaks)
        logging.debug("backward merged_streaks = %s" % backward_merged_streaks)
        logging.debug("new merged_streaks = %s" % new_merged_streaks)

        # use a separate to_remove list to avoid modifying the list and changing indices
        # while iterating
        logging.debug("before merging entries, changes were %s" %
            ([(mc[0]["idx"], mc[1]["idx"], mc[0]["type"]) for mc in self.motion_changes]))

        merged_list_p1 = self.merge_streaks_pass_1(self.motion_changes,
            forward_merged_streaks,
            backward_merged_streaks,
            new_merged_streaks)

        merged_list_p2 = self.merge_streaks_pass_2(merged_list_p1)

        logging.debug("after merging entries, changes are %s" %
            ([(mc[0]["idx"], mc[1]["idx"], mc[0]["type"]) for mc in merged_list_p2]))

        return merged_list_p2

