import json
import logging
logging.basicConfig(level=logging.DEBUG)
import argparse
import uuid

import emission.storage.decorations.user_queries as esdu
import emission.net.ext_service.push.notify_usage as pnu
import emission.net.ext_service.push.query.dispatch as pqd
import emission.core.wrapper.user as ecwu
import emission.core.get_database as edb

def get_uuid_list_for_platform(platform):
    query_fn = pqd.get_query_fn("platform")
    return query_fn({"platform": platform})

def get_upgrade_push_spec(platform):
    android_url = "https://play.google.com/store/apps/details?id=gov.nrel.cims.openpath"
    ios_url = "https://apps.apple.com/us/app/nrel-openpath/id1628058068"
    if platform == "android":
        platform_url = android_url
    elif platform == "ios":
        platform_url = ios_url
    else:
        raise InvalidArgumentException("Found unknown platform %s, expected 'android' or 'ios'" % platform)
    push_spec = {
        "alert_type": "website",
        "title": "Your version of the NREL OpenPATH app may have errors",
        "message": "Please upgrade to the most recent version",
        "image": "icon",
        "spec": {
            "url": platform_url
        }
    }
    return push_spec

def needs_version_update(uuid, target_version):
    curr_profile = edb.get_profile_db().find_one({"user_id": uuid})
    logging.debug("Read profile %s for user %s" % (curr_profile, uuid))
    if curr_profile is None:
        logging.error("Could not find profile for %s" % uuid)
        return False
    elif curr_profile["client_app_version"] == target_version:
        logging.debug("%s is already at version %s" % (uuid, curr_profile["client_app_version"]))
        return False
    else:
        logging.debug("%s is at version %s, needs update to %s" % (uuid, curr_profile["client_app_version"], target_version))
        return True

def push_upgrade_message_for_platform(platform, cli_args):
    logging.info("About to push to %s" % platform)
    uuid_list = get_uuid_list_for_platform(platform)
    logging.info("UUID list for %s = %s" % (platform, uuid_list))
    if cli_args.target_version:
        filtered_uuid_list = [uuid for uuid in uuid_list if needs_version_update(uuid, cli_args.target_version)]
        logging.info("After filtering for %s, uuid_list is %s" % (cli_args.target_version, filtered_uuid_list))
    else:
        filtered_uuid_list = uuid_list
        logging.info("No target version specified, not filtering list")
        
    spec = get_upgrade_push_spec(platform)
    if cli_args.dry_run:
        logging.info("dry run, skipping actual push")
    else:
        response = pnu.send_visible_notification_to_users(filtered_uuid_list,
                                                            spec["title"],
                                                            spec["message"],
                                                            spec,
                                                            dev = cli_args.dev)
        pnu.display_response(response)

def runTests():
    try:
        edb.get_profile_db().insert_one({"user_id": "v4", "client_app_version": "1.0.4"})
        edb.get_profile_db().insert_one({"user_id": "v5", "client_app_version": "1.0.5"})
        edb.get_profile_db().insert_one({"user_id": "v6", "client_app_version": "1.0.6"})
        assert needs_version_update("v4", "1.0.6")
        assert needs_version_update("v5", "1.0.6")
        assert not needs_version_update("v6", "1.0.6")
        assert not needs_version_update("unknown", "1.0.6")
    finally:
        logging.debug("About to delete all entries from the profile")
        edb.get_profile_db().delete_many({"user_id": "v4"})
        edb.get_profile_db().delete_many({"user_id": "v5"})
        edb.get_profile_db().delete_many({"user_id": "v6"})


if __name__ == '__main__':
    parser = argparse.ArgumentParser(prog="prompt_upgrade_to_latest")
   
    # until we figure out a way to add unit tests for scripts
    parser.add_argument("--test", action="store_true", default=False,
        help="Do everything except actually push the survey")
    parser.add_argument("-n", "--dry-run", action="store_true", default=False,
        help="Do everything except actually push the survey")
    parser.add_argument("-t", "--target-version",
        help="Only push to people who have not upgraded to this version")
    parser.add_argument("-d", "--dev", action="store_true", default=False)

    args = parser.parse_args()

    if args.test:
        runTests()
    else:
        push_upgrade_message_for_platform("android", args)
        push_upgrade_message_for_platform("ios", args)



