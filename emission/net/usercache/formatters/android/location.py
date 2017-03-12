import logging
# It is not clear if we need to copy here, given that we are almost
# immdediately going to save to the database. Let us assume that we don't.
# We can add it if it turns out that there are issues with mutability.
import copy
import attrdict as ad
import pytz
import datetime as pydt
import geojson
import arrow

import emission.core.wrapper.location as ecwl
import emission.net.usercache.formatters.common as fc
import attrdict as ad
import emission.storage.decorations.local_date_queries as ecsdlq

def format(entry):
    assert(entry.metadata.key == "background/location" or 
            entry.metadata.key == "background/filtered_location")
    if ("mLatitude" in entry.data):
        return format_location_raw(entry)
    else:
        return format_location_simple(entry)

# TODO: Remove the RAW code since we don't really have any clients that 
# are sending it. But while it is there, we should still convert ms to sec
# since the old data did have that in place
def format_location_raw(entry):
    formatted_entry = ad.AttrDict()
    formatted_entry["_id"] = entry["_id"]
    formatted_entry.user_id = entry.user_id

    metadata = entry.metadata
    metadata.time_zone = "America/Los_Angeles"
    metadata.write_ts = float(entry.metadata.write_ts)/ 1000
    fc.expand_metadata_times(metadata)
    formatted_entry.metadata = metadata

    data = ad.AttrDict()
    data.latitude = entry.data.mLatitude
    data.longitude = entry.data.mLongitude
    data.loc = geojson.Point((data.longitude, data.latitude))
    data.ts = float(entry.data.mTime) / 1000 # convert the ms from the phone to secs
    fc.expand_data_times(data, metadata)
    data.altitude = entry.data.mAltitude
    data.accuracy = entry.data.mAccuracy
    data.sensed_speed = entry.data.mSpeed
    data.heading = entry.data.mBearing
    formatted_entry.data = data

    return formatted_entry

def format_location_simple(entry):
    formatted_entry = ad.AttrDict()
    formatted_entry["_id"] = entry["_id"]
    formatted_entry.user_id = entry.user_id

    metadata = entry.metadata
    if "time_zone" not in metadata:
        metadata.time_zone = "America/Los_Angeles" 
    fc.expand_metadata_times(metadata)
    formatted_entry.metadata = metadata

    data = entry.data
    fc.expand_data_times(data, metadata)
    data.loc = geojson.Point((data.longitude, data.latitude))
    data.heading = entry.data.bearing
    del data.bearing
    formatted_entry.data = data

    return formatted_entry
