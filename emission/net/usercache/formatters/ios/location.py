import logging
# It is not clear if we need to copy here, given that we are almost
# immdediately going to save to the database. Let us assume that we don't.
# We can add it if it turns out that there are issues with mutability.
import copy
import attrdict as ad
import pytz
import datetime as pydt
import geojson

import emission.net.usercache.formatters.common as fc

def format(entry):
    assert(entry.metadata.key == "background/location")
    return format_location_simple(entry)

def format_location_simple(entry):
    formatted_entry = ad.AttrDict()
    formatted_entry["_id"] = entry["_id"]
    formatted_entry.user_id = entry.user_id

    metadata = entry.metadata
    fc.expand_metadata_times(metadata)
    formatted_entry.metadata = metadata

    data = entry.data
    local_aware_dt = pydt.datetime.utcfromtimestamp(data.ts).replace(tzinfo=pytz.utc) \
                            .astimezone(pytz.timezone(formatted_entry.metadata.time_zone))
    data.local_dt = local_aware_dt.replace(tzinfo=None)
    data.fmt_time = local_aware_dt.isoformat()
    data.loc = geojson.Point((data.longitude, data.latitude))
    data.heading = entry.data.bearing
    del data.bearing
    formatted_entry.data = data

    return formatted_entry
