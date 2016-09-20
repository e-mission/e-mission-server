import json
import bson.json_util as bju
import emission.core.get_database as edb
import argparse
import emission.core.wrapper.user as ecwu

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("timeline_filename",
        help="the name of the file that contains the json representation of the timeline")
    parser.add_argument("user_email",
        help="specify the user email to load the data as")
    args = parser.parse_args()
    fn = args.timeline_filename
    print fn
    print "Loading file " + fn
    tsdb = edb.get_timeseries_db()
    user = ecwu.User.register(args.user_email)
    override_uuid = user.uuid
    print("After registration, %s -> %s" % (args.user_email, override_uuid))
    entries = json.load(open(fn), object_hook = bju.object_hook)
    for entry in entries:
        entry["user_id"] = override_uuid
        tsdb.save(entry)
