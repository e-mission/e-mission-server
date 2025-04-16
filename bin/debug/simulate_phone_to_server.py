import sys
import logging
import uuid
import requests
import argparse

import emission.core.get_database as edb
import emission.core.wrapper.user as ecwu
import emission.storage.json_wrappers as esj

def simulate_phone_input(dest_webapp, src_uuid, dst_opcode, n_rounds=3):
    print(f"Registering {dst_opcode} at {dest_webapp} so we can push to it")
    requests.post(dest_webapp+"/profile/create", json={"user": dst_opcode})
    for i in range(n_rounds):
        print(f"About to run round {i} while copying data from {src_uuid} -> {dst_opcode} at {dest_webapp}")
        entries = list(edb.get_timeseries_db().find({"user_id": src_uuid}).sort("metadata.key", 1).limit(10000))
        for e in entries:
            del e["_id"]
            del e["user_id"]
            if "type" not in e["metadata"]:
                e["metadata"]["type"] = "missing"
        wrapped_json = {"user": dst_opcode, "phone_to_server": entries}
        print(f"Pushing {len(entries)} with keys {set([e['metadata']['key'] for e in entries])}")
        requests.post(dest_webapp+"/usercache/put", data=esj.wrapped_dumps(wrapped_json), headers={'Content-Type': 'application/json'})

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    parser = argparse.ArgumentParser(prog="simulate_phone_to_server")

    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("-a", "--all", action="store_true", help="read and write from the same user")
    group.add_argument("-s", "--single", nargs=2, help="<source_uuid> <dest_opcode>")

    parser.add_argument("dest_webapp", help="the webapp to send data to" )

    args = parser.parse_args()

    if args.single:
        src_uuid = uuid.UUID(args.single[0])
        dest_opcode = args.single[1]
        simulate_phone_input(args.dest_webapp, src_uuid, dest_opcode)
    else:
        assert args.all == True
        all_users = list(edb.get_uuid_db().find())
        for u in all_users:
            simulate_phone_input(args.dest_webapp, u["uuid"], u["user_email"])
