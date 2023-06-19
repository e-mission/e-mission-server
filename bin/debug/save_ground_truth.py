import attrdict as ad
import json
import emission.storage.json_wrappers as esj
import sys
from uuid import UUID
import argparse

import emission.core.get_database as edb
import emission.core.wrapper.user as ecwu
import emission.storage.timeseries.abstract_timeseries as esta

def save_diary(args):
    print("Saving data for %s, %s to file %s" % (args.sel_uuid, args.date, args.file_name))
    tj = edb.get_usercache_db().find_one({'metadata.key': "diary/trips-%s" % args.date, "user_id": args.sel_uuid})
    print("Retrieved object is of length %s" % len(tj))
    json.dump(tj, open(args.file_name, "w"), indent=4, default=esj.wrapped_default)

def save_ct_list(args):
    print("Saving confirmed trip list for %s to file %s" % (args.sel_uuid, args.file_name))
    ts = esta.TimeSeries.get_time_series(args.sel_uuid)
    analysis_objects = list(ts.find_entries(args.key_list, None))
    print("Retrieved object is of length %s" % len(analysis_objects))
    json.dump(analysis_objects, open(args.file_name, "w"), indent=4, default=esj.wrapped_default)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(prog="save_ground_truth")

    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("-e", "--user_email")
    group.add_argument("-u", "--user_uuid")
    subparsers = parser.add_subparsers(help="type of ground truth")

    parser_diary = subparsers.add_parser('diary', help='diary-based ground truth')
    parser_diary.add_argument("date", help="date to retrieve ground truth (YYYY-MM-DD)")
    parser_diary.set_defaults(func=save_diary)

    parser_obj_list = subparsers.add_parser('objects', help='download analysis objects directly')
    parser_obj_list.add_argument("file_name", help="file_name to store the result to")
    parser_obj_list.add_argument("key_list", nargs="+", help="list of keys to download")
    parser_obj_list.set_defaults(func=save_ct_list)

    args = parser.parse_args()

    if args.user_uuid:
        args.sel_uuid = UUID(args.user_uuid)
    else:
        args.sel_uuid = ecwu.User.fromEmail(args.user_email).uuid

    args.func(args)

