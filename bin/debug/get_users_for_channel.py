# Extracts email -> UUID mappings for a particular channel
# The UUIDs can be used to dump data for that channel to the student who
# conducted the experiment. Typically used as the file input to the
# extract_timeline_for_day_range_and_user.py script
# The channel is stored in the "client" field of the profile

import emission.core.wrapper.user as ecwu

import sys
import argparse
import logging
import json
import bson.json_util as bju

import emission.core.get_database as edb

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    parser = argparse.ArgumentParser(prog="get_users_for_channel")

    parser.add_argument("channel", help="the channel that the users signed in to")
    parser.add_argument("-o", "--outfile", help="the output filename (default: stdout)")

    args = parser.parse_args()

    matched_profiles_it = edb.get_profile_db().find({"client": args.channel})
    matched_uuids_it = [p["user_id"] for p in matched_profiles_it]
    matched_email2uuid_it = [edb.get_uuid_db().find_one({"uuid": u}) for u in matched_uuids_it]

    logging.debug("Mapped %d entries for channel %s" % (len(matched_email2uuid_it), args.channel)) 

    out_fd = sys.stdout if args.outfile is None else open(args.outfile, "w")
    json.dump(matched_email2uuid_it, out_fd, default=bju.default)
