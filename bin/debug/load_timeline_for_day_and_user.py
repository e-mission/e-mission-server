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
import emission.core.get_database as edb
import emission.core.wrapper.user as ecwu
import bin.historical.migrations.populate_profile_summaries as migration_populate_profile_summaries

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
    for i, entry in enumerate(entries):
        entry["user_id"] = override_uuid
        if not args.retain:
            del entry["_id"]
        if args.verbose is not None and i % args.verbose == 0:
            print("About to save %s" % entry)
        munged_entries.append(entry)

    (tsdb_count, ucdb_count) = estcs.insert_entries(override_uuid, munged_entries, continue_on_error=False)
    print("Finished loading %d entries into the usercache and %d entries into the timeseries" %
        (ucdb_count, tsdb_count))
    
    profile = edb.get_profile_db().find_one({"user_id": override_uuid})
    migration_populate_profile_summaries.populate_profiles([profile])
