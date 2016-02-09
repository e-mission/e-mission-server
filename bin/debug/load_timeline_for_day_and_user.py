import json
import bson.json_util as bju
import emission.core.get_database as edb
import sys
import argparse
import uuid

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("timeline_filename",
        help="the name of the file that contains the json representation of the timeline")
    parser.add_argument("-u", "--user_uuid",
        help="overwrite the user UUID from the file")
    args = parser.parse_args()
    fn = args.timeline_filename
    print fn
    print "Loading file " + fn
    tsdb = edb.get_timeseries_db()
    override_uuid = None
    if args.user_uuid is not None:
        override_uuid = uuid.uuid3(uuid.NAMESPACE_URL, "mailto:%s" % args.user_uuid.encode("UTF-8"))
    entries = json.load(open(fn), object_hook = bju.object_hook)
    for entry in entries:
        if args.user_uuid is not None:
            entry["user_id"] = override_uuid
        tsdb.save(entry)
