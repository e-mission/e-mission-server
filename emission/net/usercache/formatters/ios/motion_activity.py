import logging

import emission.core.wrapper.motionactivity as ecwa
import emission.net.usercache.formatters.common as fc
import attrdict as ad
import pandas as pd
import numpy as np

def format(entry):
    formatted_entry = ad.AttrDict()
    formatted_entry["_id"] = entry["_id"]
    formatted_entry.user_id = entry.user_id

    metadata = entry.metadata
    fc.expand_metadata_times(metadata)
    formatted_entry.metadata = metadata

    data = entry.data
    data.type = type_flags_to_enum(entry.data).value
    # TODO: for ios, the confidence is currently a string "high/medium/low".
    # should we convert it to a number to be consistent with the android version?
    # or should we leave it unchanged?
    # Let us convert it so a number so that we can try to reuse the same code
    # as android
    if 'confidence' in entry.data and 'confidence_level' not in entry.data:
        data.confidence_level = entry.data.confidence
        data.confidence = level_to_number(data.confidence_level)
    data.ts = formatted_entry.metadata.write_ts
    data.local_dt = formatted_entry.metadata.write_local_dt
    data.fmt_time = formatted_entry.metadata.write_fmt_time
    formatted_entry.data = data

    return formatted_entry

def type_flags_to_enum(data):
    # Multiple flags can be true at the same time. If we see a multiple flagged
    # entry, we should investigate it
    # 
    flags_props = ["stationary", "walking", "running",
                   "cycling", "automotive", "unknown"]
    flags_df = map_flags(data, flags_props)
    
    if np.count_nonzero(flags_df.state) == 0:
        return ecwa.MotionTypes.NONE
        
    if np.count_nonzero(flags_df.state) > 1:
        true_flags = flags_df[flags_df.state == True].flag.tolist()
        logging.info("Found two true modes %s for entry %s, skipping" % 
            (true_flags, data))
        if true_flags == ['stationary', 'automotive']:
            return ecwa.MotionTypes.STOPPED_WHILE_IN_VEHICLE
        else:
            raise RuntimeError("Cannot deal with two modes for one entry")
    else:
        # Without the last [0], we return a series with one element, which
        # means that we can't look it up easily
        true_flag = flags_df[flags_df.state == True].flag.iloc[0]
        logging.info("Found only one true mode %s, converting" % true_flag)
        return to_activity_enum(true_flag)
        
def to_activity_enum(true_flag):
    if true_flag == "stationary":
        return ecwa.MotionTypes.STILL
    if true_flag == "walking":
        return ecwa.MotionTypes.WALKING
    if true_flag == "running":
        return ecwa.MotionTypes.RUNNING
    if true_flag == "cycling":
        return ecwa.MotionTypes.BICYCLING
    if true_flag == "automotive":
        return ecwa.MotionTypes.IN_VEHICLE
    if true_flag == "unknown":
        return ecwa.MotionTypes.UNKNOWN

def level_to_number(level):
    if level == 'high':
        return 100
    if level == 'medium':
        return 75
    if level == 'low':
        return 50

def map_flags(data, flags_props):
    flags_list = []
    for prop in flags_props:
        flags_list.append({"flag": prop, "state": data[prop]})
    retVal = pd.DataFrame(flags_list)
    logging.debug("Returning dataframe = %s" % retVal)
    return retVal
