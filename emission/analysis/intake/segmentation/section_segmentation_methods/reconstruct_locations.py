import logging
import pandas as pd
import numpy as np

import emission.core.wrapper.location as ecwl
import emission.analysis.intake.cleaning.location_smoothing as eaicl
import emission.analysis.intake.location_utils as eail

FRESHNESS_DIST_THRESHOLD = 100

def get_matched_location(motion, filtered_loc_df, unfiltered_loc_df, index, timeseries):
    if len(filtered_loc_df) > 0:
        orig_matched_point = filtered_loc_df.iloc[index]
    else:
        orig_matched_point = None

    # If real points are fresh enough (within 30 secs of the motion
    # activity start, use them), otherwise, resample for this section and
    # use
    if orig_matched_point is not None:
        with_speed_filtered_df = eaicl.add_dist_heading_speed(filtered_loc_df)
        if is_fresh_point(orig_matched_point, motion, with_speed_filtered_df):
            matched_point = orig_matched_point
        else:
            start_ts, end_ts = get_time_range(motion, index, filtered_loc_df)
            resampled_df = eail.resample_for_range(filtered_loc_df, start_ts, end_ts,
                get_sample_rate(with_speed_filtered_df))
            matched_point = resampled_df.iloc[index]
            logging.debug("matched_point %s is %d secs from motion %s, using resampled location %s" % 
                (orig_matched_point.fmt_time, abs(orig_matched_point.ts - motion.ts),
                     motion.fmt_time, matched_point.fmt_time))
            matched_point = insert_resampled_point(matched_point, orig_matched_point, timeseries)
    else:
        assert False, "no filtered or unfiltered points. already skipped section..."

    return matched_point

def is_fresh_point(orig_matched_point, motion, with_speed_filtered_df):
    """
    Check to see whether the first point in the locations for this section is recent enough to be used.
    We originally used a simple threshold (30 secs), but that led to bad, split out segments, similar to 
    https://github.com/e-mission/e-mission-server/issues/577#issuecomment-377719654
    
    Instead, we use a dynamic threshold based on the speed of the section
    """
    return 30 * 60

    median_speed = with_speed_filtered_df.speed.median()
    ts_diff = abs(orig_matched_point.ts - motion.ts)
    # At this speed, how long will it take to cover that much time
    dist_diff = median_speed * ts_diff
    
    # We generally work with 100 meter thresholds, let's stick to that...
    if dist_diff < FRESHNESS_DIST_THRESHOLD:
        logging.debug("matched_point %s is %4f secs = %4f m from motion %s at speed %4f < threshold = %f, retaining" % 
            (orig_matched_point.fmt_time, ts_diff, dist_diff, motion.fmt_time, median_speed, FRESHNESS_DIST_THRESHOLD))
        return True
    else:
        logging.debug("matched_point %s is %4f secs = %4f m from motion %s at speed %4f > threshold = %f, resampling" % 
            (orig_matched_point.fmt_time, ts_diff, dist_diff, motion.fmt_time, median_speed, FRESHNESS_DIST_THRESHOLD))
        return False

def get_sample_rate(with_speed_filtered_df):
    """
    Get the rate at which we should sample to ensure that the resampled point
    is fresh enough.

    To be fresh, we want to be no more than FRESHNESS_DIST_THRESHOLD away.
    x m : 1 sec
    FDT m : FDT/x sec
    """
    floor_ts_diff = FRESHNESS_DIST_THRESHOLD // with_speed_filtered_df.speed.median()
    # but we don't want it to be zero, so make sure it is always at least 1
    ts_diff = max(1, floor_ts_diff)
    logging.debug("Returning sample rate = %d for resampling" % ts_diff)
    return ts_diff
   
def insert_resampled_point(matched_point, orig_matched_point, timeseries):
    # Make sure to insert the resampled point, so that it will exist if/when
    # we look it up later
    matched_point = matched_point.append(pd.Series({"filter": orig_matched_point.loc["filter"]}))

    # We can't store numpy ints.
    # https://github.com/e-mission/e-mission-server/issues/533#issuecomment-349067017
    for k in matched_point.index:
        if isinstance(matched_point[k], np.int64):
            matched_point[k] = float(matched_point[k])
        
    # Mark newly inserted location points as special so that we can remove them
    # later if we need to
    matched_point = matched_point.append(pd.Series({"inserted": True}))
    new_id = timeseries.insert_data(orig_matched_point.user_id, "background/filtered_location", ecwl.Location(matched_point))
    matched_point = matched_point.append(pd.Series({"_id": new_id}))
    return matched_point

def insert_resampled_entry(matched_entry, timeseries):
    import emission.storage.timeseries.builtin_timeseries as biuc

    # Make sure to insert the resampled point, so that it will exist if/when
    # we look it up later
    # Mark newly inserted location points as special so that we can remove them
    # later if we need to
    matched_entry["data"]["inserted"] = True
    matched_entry["metadata"]["key"] = "background/filtered_location"
    del matched_entry["_id"]
    new_id = timeseries.insert(matched_entry)
    matched_entry["_id"] = new_id
    matched_point = biuc.BuiltinTimeSeries._to_df_entry(dict(matched_entry))
    logging.debug("After inserting, matched point = %s" % matched_point)
    return matched_point

def get_time_range(motion, index, loc_df):
    if (index == 0):
        return (motion.ts, loc_df.ts.iloc[-1])
    else:
        assert index == -1
        return (loc_df.ts.iloc[0], motion.ts)
