from __future__ import unicode_literals
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import
# For some reason, probably because we were trying to serialize the default
# object, we put the "filter" field into the metadata. But the filter doesn't
# make sense for data other than location, so it doesn't seem like it should be
# in the metadata. Putting it into the metadata also means that it is not
# accessible as part of the data frame (although maybe we should put all
# metadata into the data frame).

# So this simple script moves the filter from the metadata into the data for
# location entries and removes it for all other entries

from future import standard_library
standard_library.install_aliases()
from builtins import *
import logging

import emission.core.get_database as edb

def get_curr_key(entry):
    return entry["metadata"]["key"]

def is_location_entry(entry):
    curr_key = get_curr_key(entry)
    return curr_key == "background/location" or curr_key == "background/filtered_location"

def move_all_filters_to_data():
    tsdb = edb.get_timeseries_db()
    for entry in tsdb.find():
        if "filter" in entry["metadata"]:
            curr_filter = entry["metadata"]["filter"]
            if is_location_entry(entry):
                entry["data"]["filter"] = curr_filter
                logging.debug("for entry %s, found key %s, moved filter %s into data" % 
                                (entry["_id"], get_curr_key(entry), curr_filter))

            # For all cases, including the location one, we want to delete the filter from metadata
            del entry["metadata"]["filter"]
            tsdb.save(entry)
            logging.debug("for entry %s, for key %s, deleted filter %s from metadata" % 
                            (entry["_id"], get_curr_key(entry), curr_filter))
        else:
            pass
            # logging.warning("No filter found for entry %s, skipping" % entry)

        if "filter" not in entry["data"] and is_location_entry(entry):
            # This must be an entry from before the time that we started sending
            # entries to the server. At that time, we only sent time entries,
            # so set it to time in this case
            entry["data"]["filter"] = "time"
            logging.debug("No entry found in either data or metadata, for key %s setting to 'time'" % entry["metadata"]["key"])
            tsdb.save(entry)
