"""
Script to launch the pipeline reset code.
Options documented in 
https://github.com/e-mission/e-mission-server/issues/333#issuecomment-312464984
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

import emission.pipeline.reset as epr
import emission.core.get_database as edb
import emission.core.wrapper.user as ecwu
import emission.storage.decorations.user_queries as esdu
import emission.core.wrapper.pipelinestate as ewps

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
    # return edb.get_timeseries_db().find({'metadata.platform': platform}).distinct(
    #    'user_id')
   return edb.get_profile_db().find({"curr_platform": platform}).distinct("user_id")

def _find_all_users():
   return esdu.get_all_uuids()

def _email_2_user_list(email_list):
    return [ecwu.User.fromEmail(e).uuid for e in email_list]

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)

    parser = argparse.ArgumentParser()
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
    parser.add_argument("-n", "--dry_run", action="store_true", default=False,
                        help="do everything except actually perform the operations")

    args = parser.parse_args()
    print(args)

    # Handle the first row in the table
    pipeline_query = {"pipeline_stage": ewps.PipelineStages.CREATE_COMPOSITE_OBJECTS.value}
    trip_query = {"metadata.key": "analysis/composite_trip"}
    if args.all:
        all_composite_states = list(edb.get_pipeline_state_db().find(pipeline_query))
        logging.info(f"About to delete {len(all_composite_states)} entries for {ewps.PipelineStages.CREATE_COMPOSITE_OBJECTS}")
        logging.debug(f"Full list is {all_composite_states}")
        logging.info(f"About to delete {edb.get_analysis_timeseries_db().count_documents(trip_query)} trips")
        if not args.dry_run:
            logging.info(f"Pipeline delete result is {edb.get_pipeline_state_db().delete_many(pipeline_query).raw_result}")
            logging.info(f"Composite trip delete result is {edb.get_analysis_timeseries_db().delete_many(trip_query).raw_result}")
    else:
        user_list = _get_user_list(args)
        logging.info("received list with %s users" % user_list)
        logging.info("first few entries are %s" % user_list[0:5])
        for user_id in user_list:
            pipeline_query['user_id'] = user_id
            trip_query['user_id'] = user_id
            user_composite_states = list(edb.get_pipeline_state_db().find(pipeline_query))
            logging.info(f"found {len(user_composite_states)} for user {user_id}")
            assert len(user_composite_states) == 1
            logging.info(f"About to delete {edb.get_analysis_timeseries_db().count_documents(trip_query)} trips")
            if not args.dry_run:
                logging.info(f"Pipeline delete result is {edb.get_pipeline_state_db().delete_many(pipeline_query).raw_result}")
                logging.info(f"Composite trip delete result is {edb.get_analysis_timeseries_db().delete_many(trip_query).raw_result}")
