# Removes all materialized views and the pipeline state.
# This will cause us to reprocess the pipeline from scratch
# As history begins to accumulate, we may want to specify a point to reset the
# pipeline to instead of deleting everything
import logging

import argparse
import uuid
import arrow
import copy
import pymongo

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
        day_dt = arrow.get(args.date, "%Y-%m-%d")
        logging.debug("day_dt is %s" % day_dt)
        day_ts = day_dt.timestamp
        logging.debug("day_ts is %s" % day_ts)
        trip_query.update({"data.start_ts": {"$gt": day_ts}})
        place_query.update({"data.exit_ts": {"$gt": day_ts}})
        point_query.update({"data.ts": {"$gt": day_ts}})

    print "trip_query = %s" % trip_query
    print "place_query = %s" % place_query
    print "point_query = %s" % point_query

    raw_trip_overlap_exclude_query = get_exclude_query("segmentation/raw_trip", trip_query)
    if raw_trip_overlap_exclude_query is not None:
        trip_query.update(raw_trip_overlap_exclude_query)
    cleaned_trip_overlap_exclude_query = get_exclude_query("segmentation/cleaned_trip", trip_query)
    if cleaned_trip_overlap_exclude_query is not None:
        trip_query.update(cleaned_trip_overlap_exclude_query)

    reset_last_place("segmentation/raw_place", place_query)
    reset_last_place("segmentation/cleaned_place", place_query)

    # When we delete objects, we want to leave an open connection to the prior
    # chain to connect the newly created chain to. In other words, if we enter
    # 2016-07-23, we want the place that we entered at 2016-07-22 to be retained
    # but with no starting trip, so that we can rejoin the newly identified trip
    # to the existing place
    # Let's think about the expected state under various scenarios:
    # - all trips ended on the 22nd
    #    - place entered on 22nd and exited on 23rd should have starting* deleted
    #    - place entered on or after 23rd deleted
    #    - trips, stops, sections started on or after 23rd deleted
    #
    # - trip overlaps 22nd -> 23rd boundary: retain the trip
    #    - first place entered after that trip (first place on the 23rd), delete starting*
    #    - delete all trips, stops, sections *started* on or after the 23rd
    #      Note also that for the overlapping trip, we should not delete stops and sections
    #      even if they started after midnight

    # So basically, we can keep the current logic all place-like objects exited
    # after the date and all trip-like objects started after the date, EXCEPT that
    # the first of those places should be reset instead of deleted, and the stops
    # sections from the last trip should be retained if it overlaps

    # Since sections have the same basic structure as trips and stops have the
    # same basic structure as places, we can reuse the queries

    print "Deleting trips/sections for %s after %s" % (args.user_id, args.date)
    print edb.get_analysis_timeseries_db().remove(trip_query)
    print "Deleting places/stops for %s after %s" % (args.user_id, args.date)
    print edb.get_analysis_timeseries_db().remove(place_query)
    print "Deleting points for %s after %s" % (args.user_id, args.date)
    print edb.get_analysis_timeseries_db().remove(point_query)

def get_exclude_query(key, query):
    first_affected_place = get_first_affected(key, query)
    if first_affected_place['data']['end_ts'] > query['data.start_ts']:
        logging.debug("overlap detected! ")

def reset_last_place(key, query):
    first_affected_place = get_first_affected(key, query)
    match_query = {"_id": first_affected_place['_id']}
    logging.debug("match query = %s" % match_query)

    reset_query = {'$unset' : {"exit_ts": "",
                               "exit_local_dt": "",
                               "exit_fmt_time": "",
                               "starting_time": "",
                               "duration": ""
                               }}
    logging.debug("reset_query = %s" % reset_query)

    edb.get_analysis_timeseries_db().update(match_query, reset_query)

    logging.debug("after update, entry is %s" %
                  edb.get_analysis_timeseries_db().find_one({'id': first_affected_place['_id']}))

def get_first_affected(key, query):
    first_affected_query = copy.copy(query)
    first_affected_query['metadata.key'] = key
    logging.debug("first_affected_query = %s" % first_affected_query)
    result_list = list(edb.get_analysis_timeseries_db().find(first_affected_query).sort('data.exit_ts').limit(1))
    if len(result_list) == 0:
        return None
    else:
        return result_list[0]

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
        day_dt = arrow.get(args.date, "%Y-%m-%d")
        logging.debug("day_dt is %s" % day_dt)
        day_ts = day_dt.timestamp
        logging.debug("day_ts is %s" % day_ts)

    stages_list = ecwp.PipelineStages
    if args.stages is not None:
        stages_list = [ecwp.PipelineStages[stage] for stage in args.stages]

    for stage in stages_list:
        reset_pipeline_for_stage(stage, user_id, day_ts)

def _find_platform_users(platform):
    return edb.get_timeseries_db().find({'metadata.platform': platform}).distinct(
        'user_id')

def del_and_reset(args):
    del_objects(args)
    reset_pipeline(args)

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)

    parser = argparse.ArgumentParser()
    parser.add_argument("user_id",
        help="user to reset the pipeline for. use 'all' for all users")
    parser.add_argument("-d", "--date",
        help="date to reset the pipeline to. Format 'YYYY-mm-dd' e.g. 2016-02-17. Interpreted in UTC, so 2016-02-17 will reset the pipeline to 2016-02-16T16:00:00-08:00 in the pacific time zone")
    parser.add_argument("-s", "--stages", choices = [stage.name for stage in ecwp.PipelineStages],
                        help="array of stage names to reset")
    parser.add_argument("-p", "--platform", choices = ['android', 'ios'],
                        help="when used with 'all' users, target only users on a particular platform")

    args = parser.parse_args()
    if args.platform is not None:
        if args.user_id != "all":
            raise argparse.ArgumentError("platform should only be used with 'all' users")
        else:
            platform_uuids = _find_platform_users(args.platform)
            for uuid in platform_uuids:
                args.user_id = uuid
                del_and_reset(args)
    else:
        del_and_reset(args)
