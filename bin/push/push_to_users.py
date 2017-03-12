import json
import logging
import argparse
import uuid

import emission.net.ext_service.push.notify_usage as pnu
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
   
    parser.add_argument("-d", "--dev", action="store_true", default=False)

    args = parser.parse_args()

    if args.user_uuid:
        uuid_list = map(lambda uuid_str: uuid.UUID(uuid_str), args.user_uuid)
    else:
        uuid_list = map(lambda uuid_str: ecwu.User.fromEmail(uuid_str).uuid, args.user_email)
    logging.info("After parsing, uuid list = %s" % uuid_list)

    if (args.silent):
        response = pnu.send_silent_notification_to_users(uuid_list, {}, dev=args.dev)
    else:
        response = pnu.send_visible_notification_to_users(uuid_list,
                                                            args.title_message[0],
                                                            args.title_message[1],
                                                            {},
                                                            dev = args.dev)
    pnu.display_response(response)
