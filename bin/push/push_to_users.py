from __future__ import unicode_literals
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import
from future import standard_library
standard_library.install_aliases()
from builtins import *
import json
import logging
import argparse
import uuid
import emission.storage.decorations.user_queries as esdu
import emission.net.ext_service.push.notify_usage as pnu
import emission.net.ext_service.push.notify_queries as pnq
import emission.core.wrapper.user as ecwu

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    parser = argparse.ArgumentParser(prog="push_to_users")
    
    msg = parser.add_mutually_exclusive_group(required=True)
    msg.add_argument("-t", "--title_message", nargs=2,
        help="specify title and message, in that order")
    msg.add_argument("-s", "--silent", action="store_true",
        help="send a silent push notification. title and message are ignored")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("-e", "--user_email", nargs="+")
    group.add_argument("-u", "--user_uuid", nargs="+")
    group.add_argument("-p", "--platform")
    group.add_argument("-a", "--all", action="store_true") 
    parser.add_argument("-d", "--dev", action="store_true", default=False)

    args = parser.parse_args()

    if args.user_uuid:
        uuid_list = [uuid.UUID(uuid_str) for uuid_str in args.user_uuid]
    elif args.platform:
        uuid_list = pnq.get_matching_user_ids(pnq.get_platform_query(args.platform))
    elif args.all:
        uuid_list = esdu.get_all_uuids() 
    else:
        uuid_list = [ecwu.User.fromEmail(uuid_str).uuid for uuid_str in args.user_email]
    logging.info("After parsing, uuid list = %s" % uuid_list)

    if (args.silent):
        response = pnu.send_silent_notification_to_users(uuid_list, {}, dev=args.dev)
    else:
        json_data = {
            "title": args.title_message[0],
            "message": args.title_message[1]
        }
        response = pnu.send_visible_notification_to_users(uuid_list,
                                                            json_data["title"],
                                                            json_data["message"],
                                                            json_data,
                                                            dev = args.dev)
    pnu.display_response(response)
