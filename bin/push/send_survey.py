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

import emission.net.ext_service.push.notify_usage as pnu
import emission.net.ext_service.push.query.dispatch as pqd
import emission.core.wrapper.user as ecwu
import emission.net.ext_service.limesurvey.limesurvey as LimeSurvey

def get_uuid_list_from_spec(query_spec_file):
    query_spec_wrapper = json.load(open(query_spec_file))
    query_fn = pqd.get_query_fn(query_spec_wrapper["query_type"])
    return query_fn(query_spec_wrapper["spec"])

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    parser = argparse.ArgumentParser(prog="push_to_users")
    
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("-e", "--user_email", nargs="+")
    group.add_argument("-u", "--user_uuid", nargs="+")
    group.add_argument("-q", "--query_spec")
   
    parser.add_argument("-d", "--dev", action="store_true", default=False)
    parser.add_argument("-s", "--show_emails", action="store_true", default=False,
        help="Display uuids -> email conversion")
    parser.add_argument("-n", "--dry-run", action="store_true", default=False,
        help="Do everything except actually push the survey")
    parser.add_argument("survey_spec",
        help="a specification for the survey that can potentially include targeting information")

    args = parser.parse_args()

    survey_spec = json.load(open(args.survey_spec))
    assert (survey_spec["alert_type"] == "survey" or survey_spec["alert_type"] == "notify"), "alert_type = %s, expected 'survey' or 'notify'" % survey_spec["alert_type"]

    type_limesurvey = False
    if "type" in survey_spec["spec"] and survey_spec["spec"]["type"] == "limesurvey":
        survey_id = survey_spec["spec"]["sid"]
        logging.info("LimeSurvey found, survey_id=%s" % survey_id)
        type_limesurvey = True
        limesurvey = LimeSurvey.get_instance()
        limesurvey.open_api()

    if args.user_uuid:
        uuid_list = [uuid.UUID(uuid_str) for uuid_str in args.user_uuid]
        if type_limesurvey:
            logging.info("About to add new participants by uuid to %s" % survey_id)
            logging.info(limesurvey.add_participants_by_uuid(survey_id, uuid_list))
    elif args.user_email:
        if type_limesurvey:
            logging.info("About to add new participants by email to %s" % survey_id)
            logging.info(limesurvey.add_participants_by_mail(survey_spec["spec"]["sid"], args.user_email))
        uuid_list = [ecwu.User.fromEmail(uuid_str).uuid for uuid_str in args.user_email]
    else:
        assert args.query_spec is not None
        uuid_list = get_uuid_list_from_spec(args.query_spec)
        if type_limesurvey:
            logging.info("About to add new participants by uuid to %s" % survey_id)
            logging.info(limesurvey.add_participants_by_uuid(survey_id, uuid_list))
            
    logging.info("About to push to uuid list = %s" % uuid_list)

    if args.show_emails:
        logging.info("About to push to email list = %s" %
            [ecwu.User.fromUUID(uuid)._User__email for uuid in uuid_list if uuid is not None])

    if args.dry_run:
        logging.info("dry run, skipping actual push")
    else:
        response = pnu.send_visible_notification_to_users(uuid_list,
                                                            survey_spec["title"],
                                                            survey_spec["message"],
                                                            survey_spec,
                                                            dev = args.dev)
        pnu.display_response(response)

    if type_limesurvey:
        limesurvey.close_api()
