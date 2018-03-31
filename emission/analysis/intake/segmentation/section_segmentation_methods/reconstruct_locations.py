import logging
import pandas as pd

import emission.core.wrapper.location as ecwl

def get_matched_location(motion, raw_section_df, resampled_sec_df, index, timeseries):
    if len(raw_section_df) > 0:
        orig_matched_point = raw_section_df.iloc[index]
    else:
        orig_matched_point = None

    # If real points are fresh enough (within 30 secs of the motion
    # activity start, use them), otherwise insert and use
    # interpolated points
    FRESHNESS_CHECK_THRESHOLD = 30
    if orig_matched_point is not None and abs(orig_matched_point.ts - motion.ts) < FRESHNESS_CHECK_THRESHOLD:
        logging.debug("matched_point %s is %d (within %d) secs from motion %s, retaining" % 
            (orig_matched_point.fmt_time, abs(orig_matched_point.ts - motion.ts), FRESHNESS_CHECK_THRESHOLD, motion.fmt_time))
        matched_point = orig_matched_point
    else:
        matched_point = resampled_sec_df.iloc[index]

        if orig_matched_point is not None:
            logging.debug("matched_point %s is %d secs from motion %s, using resampled location %s" % 
                (orig_matched_point.fmt_time, abs(orig_matched_point.ts - motion.ts),
                     motion.fmt_time, matched_point.fmt_time))
            user_id = orig_matched_point.user_id
        else:
            logging.debug("matched_point %s for motion %s, using resampled location %s" % 
                (orig_matched_point, motion.fmt_time, matched_point.fmt_time))
            user_id = timeseries.user_id

        # Make sure to insert the resampled point, so that it will exist if/when
        # we look it up later
        # TODO: Set this to the right version
        matched_point = matched_point.append(pd.Series({"filter": "distance"}))
        # Mark newly inserted location points as special so that we can move them
        # later if we need to
        matched_point = matched_point.append(pd.Series({"inserted": True}))
        new_id = timeseries.insert_data(user_id, "background/filtered_location", ecwl.Location(matched_point))
        matched_point = matched_point.append(pd.Series({"_id": new_id}))

    return matched_point
