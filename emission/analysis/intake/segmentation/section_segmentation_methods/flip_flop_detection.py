import logging
import itertools

import emission.core.wrapper.motionactivity as ecwm
import emission.core.wrapper.location as ecwl
import emission.core.common as ecc

import emission.analysis.intake.cleaning.location_smoothing as eaicl

class FlipFlopDetection():
    def __init__(self, motion_changes, seg_method):
        # List of motion activity objects representing the start and end of sections
        self.motion_changes = motion_changes
        self.seg_method = seg_method

    def is_flip_flop(self, start_motion, end_motion):
        """
        Current definition for when there is a potential flip flop
        - if the transition was based on 1 motion activity 
        """
        if end_motion["idx"] - start_motion["idx"] == 1:
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
        
        returns: +1 to merge forwards (e.g. expand the existing
                previous section forwards)
                -1 to merge backwards (e.g. expand the existing next section
                backwards)
                0 to not merge
        """

        if streak_start == streak_end:
            logging.info("Found single flip-flop %s -> %s -> %s" % 
                (streak_start - 1, streak_start, streak_start+1))
            ctv = self.check_transition_validity(streak_start, streak_end)
            cnlw = self.check_no_location_walk(streak_start, streak_end)
            if ctv != 0:
                return ctv
            if cnlw != 0:
                return cnlw
            return 0

        ics = self.is_constant_speed(streak_start, streak_end)
        cvft = self.check_valid_for_type(streak_start, streak_end)

        if ics != 0:
            return ics
        if cvft != 0:
            return cvft
        
        return 0

    def check_transition_validity(self, streak_start, streak_end):
        """
        If there is a single flip flop (e.g. WALKING -> BICYCLING -> WALKING),
        the only decision we have is whether to remove the middle transition.
        And we can include a pretty simple fix for that - the only valid
        intermediate transition is WALKING. For all other modes, we expect that
        the travel will be long enough that we will get at least a couple of
        activity points. It's not worth it otherwise
        """
        assert streak_start == streak_end, \
            "1 flip check called with streak %d -> %d" % (streak_start, streak_end)
        start_change = self.motion_changes[streak_start]
        if start_change[0].type != ecwm.MotionTypes.WALKING:
            logging.debug("single transition %s, not WALKING, merging" % start_change[0].type)
            return self.get_merge_direction(streak_start, streak_end)
        else:
            logging.debug("single transition %s, WALKING, not merging yet" % start_change[0].type)
        return 0

    def check_no_location_walk(self, streak_start, streak_end):
        assert streak_start == streak_end, \
            "1 flip check called with streak %d -> %d" % (streak_start, streak_end)
        ssm, sem = self.motion_changes[streak_start]
        streak_locs = self.seg_method.filter_points_for_range(
            self.seg_method.location_points, ssm, sem)
        streak_unfiltered_locs = self.seg_method.filter_points_for_range(
            self.seg_method.unfiltered_loc_df, ssm, sem)

        if len(streak_locs) <= 1:
            # we have no points, not even unfiltered. This must be bogus 
            return self.get_merge_direction(streak_start, streak_end)

        return 0

    def is_constant_speed(self, streak_start, streak_end):
        """
        if we have a flip + flop in the middle of the sequence
        e.g. (3,4), can we erase the 3 and merge 4 with 2?
        """
        # streak is of length 2 - e.g. (3,4) and
        # in the middle, otherwise we can't compare with what came before this streak
        start_change = self.motion_changes[streak_start]
        end_change = self.motion_changes[streak_end]
        ssm, sem = start_change
        sem, eem = end_change

        if streak_end - streak_start == 1 and \
            streak_start != 0 and \
            streak_end < len(self.motion_changes):
            before_motion = self.motion_changes[streak_start - 1]
            logging.debug("in can_erase_flip, checking merge of %s -> %s with %s -> %s" % 
                (before_motion[0].fmt_time, before_motion[1].fmt_time,
                 ssm.fmt_time, eem.fmt_time))
            streak_locs = self.seg_method.filter_points_for_range(
                self.seg_method.location_points, ssm, eem)
            if len(streak_locs) != 0:
                streak_locs.reset_index(inplace=True)
                logging.debug("in can_erase_flip, streak_locs are %s" % 
                    (streak_locs[["fmt_time", "ts"]]))
                with_speed_df = eaicl.add_dist_heading_speed(streak_locs)
                logging.debug("in can_erase_flip, speeds are %s" % 
                    (with_speed_df[["fmt_time", "speed"]]))
                with_speed_df.loc[:,"speed_pct_change"] = with_speed_df.speed.pct_change() * 100
                logging.debug("in can_erase_flip, changes are %s" % 
                    (with_speed_df[["fmt_time", "speed", "speed_pct_change"]]))
                if with_speed_df.speed_pct_change.max() < 10:
                    # TODO: Fix when we have an example
                    return self.get_merge_direction(streak_start, streak_end)
        return 0

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
        return 0


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
            # There is no before section - only one way to merge!
            logging.debug("get_merge_direction: at beginning of changes, can only merge backward")
            return -1

        before_motion = self.motion_changes[streak_start - 1]
        bsm, bem = before_motion

        if streak_end + 1 == len(self.motion_changes):
            # There is no after section - only one way to merge!
            logging.debug("get_merge_direction: at end of changes, can only merge forward")
            return 1

        after_motion = self.motion_changes[streak_end + 1]
        asm, aem = after_motion

        if bsm.type == asm.type:
            logging.debug("before type = %s, after type = %s, merge direction is don't care, returning forward"  % 
            (bsm.type, asm.type))
            return 1

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
        if (asm.type == ecwm.MotionTypes.WALKING and 
            curr_median_speed > 1.4 + 0.2 * 1.4):
            logging.debug("after is walking, but speed is %d, merge forward, returning 1" % curr_median_speed)
            return 1
        elif (bsm.type == ecwm.MotionTypes.WALKING and 
            curr_median_speed > 1.4 + 0.2 * 1.4):
            logging.debug("before is walking, but speed is %d, merge backward, returning -1")
            return -1
            
        logging.debug("while merging, comparing curr speed %s with before %s and after %s" % 
            (curr_median_speed, with_speed_points_before.speed.median(),
            with_speed_points_after.speed.median()))
        if (abs(curr_median_speed - with_speed_points_before.speed.median()) <
            abs(curr_median_speed - with_speed_points_after.speed.median())):
            # speed is closer to before than after, merge with before, merge forward
            logging.debug("before is closer, merge forward, returning 1")
            return 1
        else:
            logging.debug("after is closer, merge backward, returning -1")
            return -1

    def get_section_speed(self, loc_points, with_speed_loc_points, points_before, points_after):
        if len(loc_points) >1:
            curr_median_speed = with_speed_loc_points.speed.median()
            logging.debug("Median calculation from speeds = %s" % curr_median_speed)
        else:
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
        return curr_median_speed

    def is_valid_for_type(self, motion_change):
        """
        Basic sanity checks for the various types of movement
        Gah this is pathetic, but I am desperate here.
        """
        mcs, mce = motion_change
        validity_check_map = {
            ecwm.MotionTypes.WALKING: self.validate_walking,
            ecwm.MotionTypes.BICYCLING: self.validate_bicycling,
            ecwm.MotionTypes.IN_VEHICLE: self.validate_motorized
        }

        ret_val = validity_check_map[mcs.type](mcs, mce)
        logging.debug("Sanity checking section %s -> %s for type %s = %s" % 
            (mcs.fmt_time, mce.fmt_time, mcs.type, ret_val))
        return ret_val

    def validate_walking(self, mcs, mce):
        loc_df = self.seg_method.filter_points_for_range(
                self.seg_method.location_points, mcs, mce)
        if len(loc_df) > 0:
            loc_df.reset_index(inplace=True)
            with_speed_df = eaicl.add_dist_heading_speed(loc_df)
            if with_speed_df.speed.median() > 1.4: # preferred walking speedA
                return False
        return True

    def validate_bicycling(self, mcs, mce):
        if mce.ts - mcs.ts > 30 * 60: # thirty minutes
            return False
        return True

    def validate_motorized(self, mcs, mce):
        if mce.ts - mcs.ts < 5 * 60:
            return False
        return True

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
                                                  backward_merged_streaks):
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

        for mss, mse in forward_merged_streaks:
            # extend the pre-merge section to end with the merged section
            # by setting the end motion
            modifiable_changes[mss-1][1] = modifiable_changes[mse][1]
            # change all the merged entries to point to the retained entry
            for i in range(mss, mse+1):
                modifiable_changes[i] = modifiable_changes[mss-1]

        for mss, mse in backward_merged_streaks:
            # extend the post-merge section to start with the merged section
            # by setting the start motion
            modifiable_changes[mse+1][0] = modifiable_changes[mss][0]
            for i in range(mss, mse+1):
                modifiable_changes[i] = modifiable_changes[mse+1]

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
            ([(mc[0]["idx"], mc[1]["idx"]) for mc in self.motion_changes]))
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
        for streak in self.flip_flop_streaks:
            ss, se = streak
            sm = self.should_merge(ss, se)
            if sm == 1:
                forward_merged_streaks.append(streak)
            elif sm == -1:
                backward_merged_streaks.append(streak)

        logging.debug("forward merged_streaks = %s" % forward_merged_streaks)
        logging.debug("backward merged_streaks = %s" % backward_merged_streaks)

        # use a separate to_remove list to avoid modifying the list and changing indices
        # while iterating
        logging.debug("before merging entries, changes were %s" %
            ([(mc[0]["idx"], mc[1]["idx"]) for mc in self.motion_changes]))

        merged_list_p1 = self.merge_streaks_pass_1(self.motion_changes,
            forward_merged_streaks,
            backward_merged_streaks)

        merged_list_p2 = self.merge_streaks_pass_2(merged_list_p1)

        logging.debug("after merging entries, changes are %s" %
            ([(mc[0]["idx"], mc[1]["idx"]) for mc in merged_list_p2]))

        return merged_list_p2

