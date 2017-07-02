import logging

import argparse
import uuid
import arrow
import copy
import pymongo

import emission.core.get_database as edb
import emission.core.wrapper.pipelinestate as ecwp
import emission.storage.decorations.place_queries as esdp
import emission.storage.decorations.analysis_timeseries_queries as esda

def del_objects(args):
    del_query = {}
    if args.user_id != "all":
        del_query['user_id'] = uuid.UUID(args.user_id)

    if args.date is None:
        logging.debug("no date specified, deleting everything")
        print("Deleting all analysis information for query %s" % del_query)
        print edb.get_analysis_timeseries_db().remove(del_query)
        print edb.get_common_place_db().remove(del_query)
        print edb.get_common_trip_db().remove(del_query)
        return None
    else:
        day_dt = arrow.get(args.date, "YYYY-MM-DD")
        logging.debug("day_dt is %s" % day_dt)
        day_ts = day_dt.timestamp
        logging.debug("day_ts is %s" % day_ts)
        last_place_enter_ts = reset_pipeline_to_date(uuid.UUID(args.user_id), del_query, day_ts, args.dry_run)
        return last_place_enter_ts

def reset_pipeline_to_date(user_id, del_query, day_ts, is_dry_run=True):
    """
    When we delete objects, we want to leave an open connection to the prior
    chain to connect the newly created chain to. In other words, if we want
    to delete after  2016-07-23, we want the place that we entered at
    2016-07-22 to be retained but with no starting trip, so that we can
    rejoin the newly identified trip to the existing place
    The various use cases for this are documented under 
    https://github.com/e-mission/e-mission-server/issues/333

    But basically, it comes down to
    a) find the place before the time
    b) clear all analysis results after it
    c) open the place
    d) reset pipeline states to its enter_ts

    FYI: this is how we did the query earlier
    edb.get_analysis_timeseries_db().find(first_affected_query).sort('data.exit_ts').limit(1)
    """
    last_cleaned_place = esdp.get_last_place_before(esda.CLEANED_PLACE_KEY, day_ts, user_id)
    last_place_enter_ts = last_cleaned_place.data.enter_ts

    # handle all trip-like entries
    del_query.update({"data.start_ts": {"$gt": last_place_enter_ts}})
    # handle all place-like entries
    del_query.update({"data.enter_ts": {"$gt": last_place_enter_ts}})
    # handle all reconstructed points
    del_query.update({"data.ts": {"$gt": last_place_enter_ts}})

    logging.debug("After all updates, del_query = %s" % del_query)
    logging.info("About to delete %d entries" 
        % edb.get_analysis_timeseries_db().find(del_query).count())
    logging.info("About to delete entries with keys %s" 
        % edb.get_analysis_timeseries_db().find(del_query).distinct("metadata.key"))

    if is_dry_run:
        return

    logging.debug("Deleting analysis entries for %s after %s" % (user_id, last_place_enter_ts))
    logging.debug(edb.get_analysis_timeseries_db().remove(del_query))

    reset_last_place(last_cleaned_place)
    last_raw_place = last_cleaned_place.data.raw_places[-1]
    reset_last_place(last_raw_place)
    return last_place_enter_ts

def reset_last_place(last_place):
    match_query = {"_id": last_place['_id']}
    logging.debug("match query = %s" % match_query)
    
    # Note that we need to reset the raw_place array
    # since it will be repopulated with new squished places 
    # when the timeline after the _entry_ to this place is reconstructed
    # Note that 
    # "If the field does not exist, then $unset does nothing (i.e. no
    # operation).", so this is still OK.
    reset_query = {'$unset' : {"exit_ts": "",
                               "exit_local_dt": "",
                               "exit_fmt_time": "",
                               "starting_time": "",
                               "duration": "",
                               "raw_places": ""
                               }}
    logging.debug("reset_query = %s" % reset_query)

    edb.get_analysis_timeseries_db().update(match_query, reset_query)

    logging.debug("after update, entry is %s" %
                  edb.get_analysis_timeseries_db().find_one({'id': last_place['_id']}))

def reset_pipeline_for_stage(stage, user_id, day_ts):
    reset_query = {}

    if user_id is not None:
        if day_ts is None:
            print "day_ts is None, deleting stage %s for user %s" % (stage, user_id)
            print edb.get_pipeline_state_db().remove({'user_id': user_id,
                    'pipeline_stage': stage.value})
        else:
            print "day_ts is %s, setting stage %s for user %s" % (day_ts, stage, user_id)
            print edb.get_pipeline_state_db().update(
                    {'user_id': user_id, 'pipeline_stage': stage.value},
                    {'$set': {'last_processed_ts': day_ts}}, upsert=False)
    else:
        if day_ts is None:
            print "day_ts is None, deleting stage %s for all users" % (stage)
            print edb.get_pipeline_state_db().remove({'pipeline_stage': stage.value})
        else:
            print "day_ts is %s, setting stage %s for all users" % (day_ts, stage)
            print edb.get_pipeline_state_db().update(
                    {'pipeline_stage': stage.value},
                    {'$set': {'last_processed_ts': day_ts}}, upsert=False)

def reset_pipeline(args, last_place_enter_ts):
    user_id = None
    if args.user_id != "all":
        user_id = uuid.UUID(args.user_id)

    stages_list = ecwp.PipelineStages
    if args.stages is not None:
        stages_list = [ecwp.PipelineStages[stage] for stage in args.stages]

    for stage in stages_list:
        reset_pipeline_for_stage(stage, user_id, last_place_enter_ts)

def _find_platform_users(platform):
    return edb.get_timeseries_db().find({'metadata.platform': platform}).distinct(
        'user_id')

def del_and_reset(args):
    last_place_enter_ts = del_objects(args)
    reset_pipeline(args, last_place_enter_ts)

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
    parser.add_argument("-n", "--dry-run", action="store_true", default=False,
                        help="do everything except actually perform the operations")

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
