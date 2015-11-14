# For some reason, probably because we were trying to serialize the default
# object, we put the "filter" field into the metadata. But the filter doesn't
# make sense for data other than location, so it doesn't seem like it should be
# in the metadata. Putting it into the metadata also means that it is not
# accessible as part of the data frame (although maybe we should put all
# metadata into the data frame).

# So this simple script moves the filter from the metadata into the data for
# location entries and removes it for all other entries

import logging

import emission.core.get_database as edb

def move_all_filters_to_data():
    for entry in edb.get_timeseries_db().find():
        if "filter" in entry["metadata"]:
            curr_key = entry["metadata"]["key"]
            curr_filter = entry["metadata"]["filter"]
            if curr_key == "background/location" or curr_key == "background/filtered_location":
                entry["data"]["filter"] = curr_filter
                logging.debug("for entry %s, found key %s, moved filter %s into data" % 
                                (entry["_id"], curr_key, curr_filter))

            # For all cases, including the location one, we want to delete the filter from metadata
            del entry["metadata"]["filter"]
            edb.get_timeseries_db().save(entry)
            logging.debug("for entry %s, for key %s, deleted filter %s from metadata" % 
                            (entry["_id"], curr_key, curr_filter))
        else:
            logging.warning("No filter found for entry %s, skipping" % entry)
