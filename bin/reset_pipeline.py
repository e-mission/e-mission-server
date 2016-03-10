# Removes all materialized views and the pipeline state.
# This will cause us to reprocess the pipeline from scratch
# As history begins to accumulate, we may want to specify a point to reset the
# pipeline to instead of deleting everything
import logging
logging.basicConfig(level=logging.DEBUG)

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
        trip_query = del_query
        place_query = del_query
    else:
        day_dt = pydt.datetime.strptime(args.date, "%Y-%m-%d")
        logging.debug("day_dt is %s" % day_dt)
        day_ts = time.mktime(day_dt.timetuple())
        logging.debug("day_ts is %s" % day_ts)
        trip_query = copy.copy(del_query)
        trip_query.update({"start_ts": {"$gt": day_ts}})
        place_query = copy.copy(del_query)
        place_query.update({"exit_ts": {"$gt": day_ts}})

    print "trip_query = %s" % trip_query
    print "place_query = %s" % place_query

    # Since sections have the same basic structure as trips and stops have the
    # same basic structure as places, we can reuse the queries
    print "Deleting trips for %s after %s" % (args.user_id, args.date)
    print edb.get_trip_new_db().remove(trip_query)
    print "Deleting sections for %s after %s" % (args.user_id, args.date)
    print edb.get_section_new_db().remove(trip_query)
    print "Deleting places for %s after %s" % (args.user_id, args.date)
    print edb.get_place_db().remove(place_query)
    print "Deleting stops for %s after %s" % (args.user_id, args.date)
    print edb.get_stop_db().remove(place_query)

def reset_pipeline_for_stage(stage, user_id, day_ts):
    reset_query = {}

    if user_id is not None:
        if day_ts is not None:
            print "Setting new pipeline stage %s for %s to %d" % (stage, user_id, day_ts)
            print edb.get_pipeline_state_db().update({'user_id': user_id,
                    'pipeline_stage': stage.value},
                    {'$set': {'last_processed_ts': day_ts}}, upsert=False)
        else:
            print "day_ts is None, deleting stage %s for user %s" % (stage, user_id)
            print edb.get_pipeline_state_db().remove({'user_id': user_id,
                    'pipeline_stage': stage.value})
    else:
        if day_ts is not None:
            print "Setting new pipeline stage %s for all users to %d" % (stage, day_ts)
            print edb.get_pipeline_state_db().update({'pipeline_stage': stage.value},
                    {'$set': {'last_processed_ts': day_ts}}, upsert=False)
        else:
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
    parser = argparse.ArgumentParser()
    parser.add_argument("user_id",
        help="user to reset the pipeline for. use 'all' for all users")
    parser.add_argument("-d", "--date",
        help="date to reset the pipeline to. Format 'YYYY-mm-dd' e.g. 2016-02-17. Interpreted in UTC, so 2016-02-17 will reset the pipeline to 2016-02-20T08:00:00 in the pacific time zone")

    args = parser.parse_args()
    del_objects(args)
    reset_pipeline(args)
