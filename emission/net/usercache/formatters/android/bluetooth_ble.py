import logging

import emission.core.wrapper.bluetoothble as ecwb
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

    #logging.info('*** Motion Data write_ts: %d' % metadata.write_ts)
   
    data = entry.data
    data.local_dt = formatted_entry.metadata.write_local_dt
    data.fmt_time = formatted_entry.metadata.write_fmt_time
    data.eventType = ecwb.BLEEventTypes[entry.data.eventType].value
    formatted_entry.data = data

    return formatted_entry
