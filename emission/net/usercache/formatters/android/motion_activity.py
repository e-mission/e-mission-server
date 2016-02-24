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
    fc.expand_metadata_times(metadata)
    formatted_entry.metadata = metadata

    data = ad.AttrDict()
    if 'agb' in entry.data:
        data.type = ecwa.MotionTypes(entry.data.agb).value
    else:
        data.type = ecwa.MotionTypes(entry.data.zzaEg).value

    if 'agc' in entry.data:
        data.confidence = entry.data.agc
    else:
        data.confidence = entry.data.zzaEh

    data.ts = formatted_entry.metadata.write_ts
    data.local_dt = formatted_entry.metadata.write_local_dt
    data.fmt_time = formatted_entry.metadata.write_fmt_time
    formatted_entry.data = data

    return formatted_entry
