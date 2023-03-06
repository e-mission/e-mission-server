from __future__ import division
from __future__ import unicode_literals
from __future__ import print_function
from __future__ import absolute_import
from future import standard_library
standard_library.install_aliases()
from builtins import *
from past.utils import old_div
import logging
# It is not clear if we need to copy here, given that we are almost
# immdediately going to save to the database. Let us assume that we don't.
# We can add it if it turns out that there are issues with mutability.
import copy
import attrdict as ad
import pytz
import datetime as pydt
import geojson
geojson.geometry.Geometry.__init__.__defaults__ = (None, False, 15)
import arrow

import emission.core.wrapper.location as ecwl
import emission.net.usercache.formatters.common as fc
import attrdict as ad
import emission.storage.decorations.local_date_queries as ecsdlq

def format(entry):
    assert(entry.metadata.key == "background/location" or 
            entry.metadata.key == "background/filtered_location")
    return format_location_simple(entry)

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
