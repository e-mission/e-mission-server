import logging

import emission.core.wrapper.motionactivity as ecwa
import emission.net.usercache.formatters.common as fc
import attrdict as ad

def format(entry):
    formatted_entry = ad.AttrDict()
    formatted_entry["_id"] = entry["_id"]
    formatted_entry.user_id = entry.user_id

    metadata = entry.metadata
    if "time_zone" not in metadata:
        metadata.time_zone = "America/Los_Angeles" 
    metadata.write_ts = float(entry.metadata.write_ts) / 1000
    fc.expand_metadata_times(metadata)
    formatted_entry.metadata = metadata

    data = ad.AttrDict()
    data.type = ecwa.MotionTypes(entry.data.agb).value
    data.confidence = entry.data.agc
    data.ts = formatted_entry.metadata.write_ts
    data.fmt_time = formatted_entry.metadata.write_fmt_time
    formatted_entry.data = data

    return formatted_entry
