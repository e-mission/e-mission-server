import logging
import common
import argparse
import emission.core.wrapper.user as ecwu

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)

    parser = argparse.ArgumentParser(prog="purge_user")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("-e", "--user_email")
    group.add_argument("-u", "--user_uuid")

    parser.add_argument("-p", "--pipeline-purge", default=False, action='store_true',
        help="purge the pipeline state as well")

    args = parser.parse_args()

    if args.user_uuid:
        sel_uuid = uuid.UUID(args.user_uuid)
    else:
        sel_uuid = ecwu.User.fromEmail(args.user_email).uuid

    common.purge_entries_for_user(sel_uuid, args.pipeline_purge)
