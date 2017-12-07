"""
Script to launch the pipeline reset code.
Options documented in 
https://github.com/e-mission/e-mission-server/issues/333#issuecomment-312464984

Can be made a little less general but a lot more performant by using the trick
below.
"""
from __future__ import print_function
from __future__ import unicode_literals
from __future__ import division
from __future__ import absolute_import
from future import standard_library
standard_library.install_aliases()
from builtins import *
import logging

import argparse
import uuid
import arrow
import copy
import pymongo

import emission.net.ext_service.habitica.executor as enehe
import emission.core.get_database as edb

def _get_user_list(args):
    if args.all:
        return _find_all_users()
    elif args.platform:
        return _find_platform_users(args.platform)
    elif args.email_list:
        return _email_2_user_list(args.email_list)
    else:
        assert args.user_list is not None
        return [uuid.UUID(u) for u in args.user_list]

def _find_platform_users(platform):
    # Since all new clients register a profile with the server, we don't have
    # to run a 'distinct' query over the entire contents of the timeseries.
    # Instead, we can simply query from the profile users, which is
    # significantly faster
    # Use the commented out line instead for better performance.
    # Soon, we can move to the more performant option, because there will be
    # no users that don't have a profile
    # return edb.get_profile_db().find({"curr_platform": platform}).distinct("user_id")
   return edb.get_timeseries_db().find({'metadata.platform': platform}).distinct(
       'user_id')

def _find_all_users():
   return edb.get_timeseries_db().find().distinct('user_id')

def _email_2_user_list(email_list):
    return [ecwu.User.fromEmail(e) for e in email_list]

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)

    parser = argparse.ArgumentParser(description="Reset the habitica pipeline.  Does NOT delete points, so to avoid double counting, use only in situations where the original run would not have given any points")
    # Options corresponding to
    # https://github.com/e-mission/e-mission-server/issues/333#issuecomment-312464984
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("-a", "--all", action="store_true", default=False,
        help="reset the pipeline for all users")
    group.add_argument("-p", "--platform", choices = ['android', 'ios'],
                        help="reset the pipeline for all on the specified platform")
    group.add_argument("-u", "--user_list", nargs='+',
        help="user ids to reset the pipeline for")
    group.add_argument("-e", "--email_list", nargs='+',
        help="email addresses to reset the pipeline for")
    parser.add_argument("date",
        help="date to reset the pipeline to. Format 'YYYY-mm-dd' e.g. 2016-02-17. Interpreted in UTC, so 2016-02-17 will reset the pipeline to 2016-02-16T16:00:00-08:00 in the pacific time zone")
    parser.add_argument("-n", "--dry_run", action="store_true", default=False,
                        help="do everything except actually perform the operations")

    args = parser.parse_args()
    print(args)

    print("Resetting timestamps to %s" % args.date)
    print("WARNING! Any points awarded after that date will be double counted!")
    # Handle the first row in the table
    day_dt = arrow.get(args.date, "YYYY-MM-DD")
    logging.debug("day_dt is %s" % day_dt)
    day_ts = day_dt.timestamp
    logging.debug("day_ts is %s" % day_ts)
    user_list = _get_user_list(args)
    logging.info("received list with %s users" % user_list)
    logging.info("first few entries are %s" % user_list[0:5])
    for user_id in user_list:
        logging.info("resetting user %s to ts %s" % (user_id, day_ts))
        enehe.reset_all_tasks_to_ts(user_id, day_ts, args.dry_run)

