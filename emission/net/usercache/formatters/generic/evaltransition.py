import logging

import emission.core.wrapper.evaltransition as et
import emission.net.usercache.formatters.common as fc
import attrdict as ad

def format(entry):
    formatted_entry = ad.AttrDict()
    formatted_entry["_id"] = entry["_id"]
    formatted_entry.user_id = entry.user_id
    
    m = entry.metadata
    if "time_zone" not in m:
        m.time_zone = "America/Los_Angeles" 
    logging.debug("Timestamp conversion: %s -> %s done" % (entry.metadata.write_ts, m.write_ts))
    fc.expand_metadata_times(m)
    formatted_entry.metadata = m

    data = ad.AttrDict(entry.data)
    data.transition = et.TransitionType[entry.data.transition].value
    fc.expand_data_times(data, m)
    formatted_entry.data = data

    return formatted_entry
