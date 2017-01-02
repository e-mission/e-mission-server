# Removes all materialized views and the pipeline state.
# This will cause us to reprocess the pipeline from scratch
# As history begins to accumulate, we may want to specify a point to reset the
# pipeline to instead of deleting everything
import logging

import argparse
import uuid
import datetime as pydt
import time
import copy

import emission.core.get_database as edb
import emission.core.wrapper.pipelinestate as ecwp

def del_objects(args):
    del_query = {}
    if args.user_id != "all":
        del_query['user_id'] = uuid.UUID(args.user_id)

    if args.date is None:
        print("Deleting all analysis information for query %s" % del_query)
        print edb.get_analysis_timeseries_db().remove(del_query)
        print edb.get_common_place_db().remove(del_query)
        print edb.get_common_trip_db().remove(del_query)

def reset_pipeline_for_stage(stage, user_id, day_ts):
    reset_query = {}

    if user_id is not None:
        if day_ts is None:
            print "day_ts is None, deleting stage %s for user %s" % (stage, user_id)
            print edb.get_pipeline_state_db().remove({'user_id': user_id,
                    'pipeline_stage': stage.value})
    else:
        if day_ts is None:
            print "day_ts is None, deleting stage %s for all users" % (stage)
            print edb.get_pipeline_state_db().remove({'pipeline_stage': stage.value})

def reset_pipeline(args):
    user_id = None
    if args.user_id != "all":
        user_id = uuid.UUID(args.user_id)

    day_ts = None
    if args.date is not None:
        day_dt = pydt.datetime.strptime(args.date, "%Y-%m-%d")
        logging.debug("day_dt is %s" % day_dt)
        day_ts = time.mktime(day_dt.timetuple())
        logging.debug("day_ts is %s" % day_ts)

    for stage in ecwp.PipelineStages:
        reset_pipeline_for_stage(stage, user_id, day_ts)

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)

    parser = argparse.ArgumentParser()
    parser.add_argument("user_id",
        help="user to reset the pipeline for. use 'all' for all users")

    args = parser.parse_args()
    # Hardcoding this for now until we reset the pipeline again
    args.date = None
    del_objects(args)
    reset_pipeline(args)
