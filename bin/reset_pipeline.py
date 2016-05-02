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

    trip_query = copy.copy(del_query)
    trip_query.update({"metadata.key": {
        "$in": ["segmentation/raw_trip", "analysis/cleaned_trip",
                "segmentation/raw_section", "analysis/cleaned_section"]}})

    place_query = copy.copy(del_query)
    place_query.update({"metadata.key": {
        "$in": ["segmentation/raw_place", "analysis/cleaned_place",
                "segmentation/raw_stop", "analysis/cleaned_stop"]}})

    point_query = copy.copy(del_query)
    point_query.update({"metadata.key": {
        "$in": ["analysis/recreated_location"]}})

    if args.date is None:
        logging.debug("no date specified, deleting everything")
    else:
        day_dt = pydt.datetime.strptime(args.date, "%Y-%m-%d")
        logging.debug("day_dt is %s" % day_dt)
        day_ts = time.mktime(day_dt.timetuple())
        logging.debug("day_ts is %s" % day_ts)
        trip_query.update({"data.start_ts": {"$gt": day_ts}})
        place_query.update({"data.exit_ts": {"$gt": day_ts}})
        point_query.update({"data.ts": {"$gt": day_ts}})

    print "trip_query = %s" % trip_query
    print "place_query = %s" % place_query
    print "point_query = %s" % point_query

    # Since sections have the same basic structure as trips and stops have the
    # same basic structure as places, we can reuse the queries
    print "Deleting trips/sections for %s after %s" % (args.user_id, args.date)
    print edb.get_analysis_timeseries_db().remove(trip_query)
    print "Deleting places/stops for %s after %s" % (args.user_id, args.date)
    print edb.get_analysis_timeseries_db().remove(place_query)
    print "Deleting points for %s after %s" % (args.user_id, args.date)
    print edb.get_analysis_timeseries_db().remove(point_query)

def reset_pipeline_for_stage(stage, user_id, day_ts):
    reset_query = {}

    if user_id is not None:
        if day_ts is not None:
            print "Setting new pipeline stage %s for %s to %d" % (stage, user_id, day_ts)
            print edb.get_pipeline_state_db().update({'user_id': user_id,
                    'pipeline_stage': stage.value},
                    {'$set': {'last_processed_ts': day_ts}}, upsert=False)
            print edb.get_pipeline_state_db().update({'user_id': user_id,
                    'pipeline_stage': stage.value},
                    {'$set': {'curr_run_ts': None}}, upsert=False)
        else:
            print "day_ts is None, deleting stage %s for user %s" % (stage, user_id)
            print edb.get_pipeline_state_db().remove({'user_id': user_id,
                    'pipeline_stage': stage.value})
    else:
        if day_ts is not None:
            print "Setting new pipeline stage %s for all users to %d" % (stage, day_ts)
            print edb.get_pipeline_state_db().update({'pipeline_stage': stage.value},
                    {'$set': {'last_processed_ts': day_ts}}, upsert=False)
            print edb.get_pipeline_state_db().update({'pipeline_stage': stage.value},
                    {'$set': {'curr_run_ts': day_ts}}, upsert=False)
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
