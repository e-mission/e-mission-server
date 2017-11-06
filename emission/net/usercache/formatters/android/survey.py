import logging
import arrow

import emission.net.usercache.formatters.common as fc
import emission.storage.decorations.local_date_queries as ecsdlq
import attrdict as ad

def format(entry):
    formatted_entry = ad.AttrDict()
    formatted_entry["_id"] = entry["_id"]
    formatted_entry.user_id = entry.user_id

    metadata = entry.metadata
    if "time_zone" not in metadata:
        metadata.time_zone = "America/Los_Angeles"
    if "write_ts" in metadata:
        logging.debug("Timestamp conversion: %s -> %s done" % (metadata.write_ts, metadata.write_ts))
        fc.expand_metadata_times(metadata)
    formatted_entry.metadata = metadata

    data = entry.data
    if "write_ts" in metadata:
        fc.expand_data_times(data, metadata)
    formatted_entry.data = data

    return formatted_entry
