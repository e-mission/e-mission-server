import arrow
import logging
import json
import argparse
from uuid import UUID
import emission.core.get_database as edb

def find_last_get(uuid):
    last_get_result_list = list(edb.get_timeseries_db().find({"user_id": uuid,
        "metadata.key": "stats/server_api_time",
        "data.name": "POST_/usercache/get"}).sort("data.ts", -1).limit(1))
    last_get = last_get_result_list[0] if len(last_get_result_list) > 0 else None
    return last_get

def check_active(uuid_list, threshold):
    now = arrow.get().timestamp
    last_get_entries = [find_last_get(npu) for npu in uuid_list]
    for uuid, lge in zip(uuid_list, last_get_entries):
        if lge is None:
            print(uuid, None, "inactive")
        else:
            last_call_diff = arrow.get().timestamp - lge["metadata"]["write_ts"]
            if last_call_diff > threshold:
                print(uuid, lge["metadata"]["write_fmt_time"], "inactive")
            else:
                print(uuid, lge["metadata"]["write_fmt_time"], "active")

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    parser = argparse.ArgumentParser(prog="find_active_users")

    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("-e", "--user_email", nargs="+")
    group.add_argument("-u", "--user_uuid", nargs="+")
    group.add_argument("-a", "--all", action="store_true")
    group.add_argument("-f", "--file")

    args = parser.parse_args()

    if args.user_uuid:
        uuid_list = [uuid.UUID(uuid_str) for uuid_str in args.user_uuid]
    elif args.user_email:
        uuid_list = [ecwu.User.fromEmail(uuid_str).uuid for uuid_str in args.user_email]
    elif args.all:
        uuid_list = esdu.get_all_uuids()
    elif args.file:
        with open(args.file) as fd:
            uuid_strings = fd.readlines()
            uuid_list = [UUID(us.strip()) for us in uuid_strings]
    ONE_WEEK = 7 * 24 * 60 * 60
    check_active(uuid_list, ONE_WEEK)
