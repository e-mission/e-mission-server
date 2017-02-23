import json
import logging
import argparse
import uuid

import emission.net.ext_service.push.notify_usage as pnu
import emission.core.wrapper.user as ecwu

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    parser = argparse.ArgumentParser(prog="push_to_users")
    
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("-e", "--user_email", nargs="+")
    group.add_argument("-u", "--user_uuid", nargs="+")
    # group.add_argument("-q", "--query_based", action="store_true")
   
    parser.add_argument("-d", "--dev", action="store_true", default=False)
    parser.add_argument("survey_spec",
        help="a specification for the survey that can potentially include targeting information")

    args = parser.parse_args()

    survey_spec = json.load(open(args.survey_spec))
    # assert(survey_spec["alert_type"] == "survey",
    #     "alert_type = %s, expected 'survey'" % survey_spec["alert_type"])

    if args.user_uuid:
        uuid_list = map(lambda uuid_str: uuid.UUID(uuid_str), args.user_uuid)
    else:
        uuid_list = map(lambda uuid_str: ecwu.User.fromEmail(uuid_str).uuid, args.user_email)
        
    logging.info("After parsing, uuid list = %s" % uuid_list)

    response = pnu.send_visible_notification_to_users(uuid_list,
                                                        survey_spec["title"],
                                                        survey_spec["message"],
                                                        survey_spec,
                                                        dev = args.dev)
    pnu.display_response(response)
