import logging

import emission.core.wrapper.motionactivity as ecwm

#
# If the timeDiff is large but the distanceDiff is small, we could either have
# the situation where:
# - the user is moving
# - ios is giving us a spurious point near the last point
# We assume that it is the second case iff:
# - we don't see any transitions here
# - there is ongoing motion between the two points

def is_huge_invalid_ts_offset(filterMethod, lastPoint, currPoint, timeseries,
                              motionInRange):
    intermediate_transitions = filterMethod.transition_df[
                                    (filterMethod.transition_df.ts >= lastPoint.ts) &
                                    (filterMethod.transition_df.ts <= currPoint.ts)]

    ignore_modes_list = [ecwm.MotionTypes.TILTING.value,
                        ecwm.MotionTypes.UNKNOWN.value,
                        ecwm.MotionTypes.STILL.value,
                        ecwm.MotionTypes.NONE.value,
                        ecwm.MotionTypes.STOPPED_WHILE_IN_VEHICLE.value]

    non_still_motions = [ma for ma in motionInRange if ma["data"]["type"] not in ignore_modes_list and ma["data"]["confidence"] == 100] 
    logging.debug("non_still_motions = %s" % [(ecwm.MotionTypes(ma["data"]["type"]), ma["data"]["confidence"], ma["data"]["fmt_time"]) for ma in non_still_motions])

    non_still_motions_rate = len(non_still_motions) / (currPoint.ts - lastPoint.ts)

    logging.debug("in is_huge_invalid_ts_offset: len(intermediate_transitions) = %d, non_still_motions = %d, time_diff = %s mins, non_still_motions_rate = %s" % (len(intermediate_transitions), len(non_still_motions), (currPoint.ts - lastPoint.ts)/60, non_still_motions_rate))
     
    # If we have no transitions and at least one high confidence motion
    # activity every 5 minutes, we claim that we were actually moving during the
    # interim and the currPoint is invalid
    if len(intermediate_transitions) == 0 and non_still_motions_rate > 1/(5 * 60):
        logging.debug("in is_huge_invalid_ts_offset: returning True")
        return True
    else:
        logging.debug("in is_huge_invalid_ts_offset: returning False")
        return False
