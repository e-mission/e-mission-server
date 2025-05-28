from __future__ import print_function
from __future__ import unicode_literals
from __future__ import division
from __future__ import absolute_import
from future import standard_library
standard_library.install_aliases()
from builtins import *
import json
import emission.storage.json_wrappers as esj
import emission.storage.timeseries.cache_series as estcs
import argparse
import emission.core.wrapper.user as ecwu
import arrow
import logging
import emission.analysis.result.user_stat as earus

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("timeline_filename",
        help="the name of the file that contains the json representation of the timeline")
    parser.add_argument("user_email",
        help="specify the user email to load the data as")

    parser.add_argument("-r", "--retain", action="store_true",
        help="specify whether the entries should overwrite existing ones (default) or create new ones")

    parser.add_argument("-v", "--verbose", type=int,
        help="after how many lines we should print a status message.")

    args = parser.parse_args()
    fn = args.timeline_filename
    print(fn)
    print("Loading file " + fn)
    user = ecwu.User.register(args.user_email)
    override_uuid = user.uuid
    print("After registration, %s -> %s" % (args.user_email, override_uuid))
    entries = json.load(open(fn), object_hook = esj.wrapped_object_hook)
    munged_entries = []
    
    # Find the most recent location timestamp
    last_location_ts = 0
    last_phone_data_ts = 0
    
    for i, entry in enumerate(entries):
        entry["user_id"] = override_uuid
        if not args.retain:
            del entry["_id"]
        if args.verbose is not None and i % args.verbose == 0:
            print("About to save %s" % entry)
        
        # Track the latest timestamps
        if "metadata" in entry and "key" in entry["metadata"] and entry["metadata"]["key"] == "background/location" and "data" in entry and "ts" in entry["data"]:
            if entry["data"]["ts"] > last_location_ts:
                last_location_ts = entry["data"]["ts"]
        
        # Track phone data timestamp (metadata write_ts)
        if "metadata" in entry and "write_ts" in entry["metadata"]:
            if entry["metadata"]["write_ts"] > last_phone_data_ts:
                last_phone_data_ts = entry["metadata"]["write_ts"]
        
        munged_entries.append(entry)

    (tsdb_count, ucdb_count) = estcs.insert_entries(override_uuid, munged_entries, continue_on_error=False)
    print("Finished loading %d entries into the usercache and %d entries into the timeseries" %
        (ucdb_count, tsdb_count))
    
    # Update the profile with the latest timestamps
    if last_location_ts > 0:
        print(f"Updating profile with last_location_ts: {arrow.get(last_location_ts)}")
        earus.update_upload_timestamp(override_uuid, "last_location_ts", last_location_ts)
    
    if last_phone_data_ts > 0:
        print(f"Updating profile with last_phone_data_ts: {arrow.get(last_phone_data_ts)}")
        earus.update_upload_timestamp(override_uuid, "last_phone_data_ts", last_phone_data_ts)
    
    # Also update pipeline_range to ensure it's properly set
    # Set it to a value in the past to ensure processing can start
    user_profile = user.getProfile()
    if "pipeline_range" not in user_profile or user_profile.get("pipeline_range", {}).get("end_ts", 0) == 0:
        print("Setting initial pipeline_range to ensure processing runs")
        initial_ts = 0  # Unix epoch start
        user.update({"pipeline_range": {"end_ts": initial_ts}})
    
    print("Profile updates complete. Your data should now be processable by the pipeline.")
