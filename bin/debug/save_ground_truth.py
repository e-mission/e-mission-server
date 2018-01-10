import emission.core.get_database as edb
import attrdict as ad
import json
import bson.json_util as bju
import sys
from uuid import UUID
import argparse

import emission.core.wrapper.user as ecwu

if __name__ == '__main__':
    parser = argparse.ArgumentParser(prog="save_ground_truth")

    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("-e", "--user_email")
    group.add_argument("-u", "--user_uuid")
   
    parser.add_argument("date", help="date to retrieve ground truth (YYYY-MM-DD)")
    parser.add_argument("file_name", help="file_name to store the result to")

    args = parser.parse_args()

    if args.user_uuid:
        sel_uuid = uuid.UUID(args.user_uuid)
    else:
        sel_uuid = ecwu.User.fromEmail(args.user_email).uuid

    print("Saving data for %s, %s to file %s" % (sel_uuid, args.date, args.file_name))
    tj = edb.get_usercache_db().find_one({'metadata.key': "diary/trips-%s" % args.date, "user_id": sel_uuid})
    print("Retrieved object is of length %s" % len(tj))
    json.dump(tj, open(args.file_name, "w"), indent=4, default=bju.default)
