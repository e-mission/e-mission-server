import logging

import emission.core.get_database as edb
import emission.core.wrapper.pipelinestate as ecwp
import emission.storage.decorations.place_queries as esdp
import emission.storage.decorations.analysis_timeseries_queries as esda

"""
Resets the pipeline.
Options for:
- resetting the pipeline for all users versus a single user
- resetting the complete pipeline versus only after a certain time

Need to mix and match those properly.
Design documented at https://github.com/e-mission/e-mission-server/issues/333#issuecomment-312464984
"""

# 
# START: reset_user_to_ts
# 

def reset_user_to_ts(user_id, ts, is_dry_run):
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
    # Find the place before the time
    last_cleaned_place = esdp.get_last_place_before(esda.CLEANED_PLACE_KEY, day_ts, user_id)
    last_place_enter_ts = last_cleaned_place.data.enter_ts

    # clear all analysis results after it
    del_objects_after(user_id, last_place_enter_ts, is_dry_run)

    # open the raw and cleaned places
    reset_last_place(last_cleaned_place, is_dry_run)
    last_raw_place = last_cleaned_place.data.raw_places[-1]
    reset_last_place(last_raw_place, is_dry_run)

    # reset pipeline states to its enter_ts
    reset_pipeline_state(user_id, last_place_enter_ts, is_dry_run)

def del_objects_after(user_id, last_place_enter_ts, is_dry_run):
    del_query = {}
    # handle the user
    del_query.update({"user_id": user_id})
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
        logging.info("this is a dry-run, returning from del_objects_after without modifying anything")
    else:
        result = edb.get_analysis_timeseries_db().remove(del_query)
        logging.info("this is not a dry-run, result of deleting analysis entries is %s" % result)

def reset_last_place(last_place):
    if is_dry_run:
        logging.info("this is a dry-run, returning from reset_last_place without modifying anything" )
        return

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

def reset_pipeline_state(user_id, last_place_enter_ts, is_dry_run):
    stages_list = ecwp.PipelineStages
    reset_pipeline_query = {'user_id': user_id, 'last_processed_ts': {'$ne': None}}
    update_pipeline_query = {'$set': {'last_processed_ts': day_ts}}
    logging.info("out of %s total, resetting %s pipeline states for user %s to %s" % 
            (edb.get_pipeline_state_db().find({'user_id': user_id}).count(),
            edb.get_pipeline_state_db().find(reset_pipeline_query).count(),
            user_id, last_place_enter_ts))
    if is_dry_run:
        logging.info("this is a dry run, returning from reset_pipeline_state without modifying anything")
    else:
        result = edb.get_pipeline_state_db().update(
                    reset_pipeline_query, update_pipeline_query,
                    upsert=False)
        logging.debug("this is not a dry run, result of update in reset_pipeline_state = %s" % result)

# 
# END: reset_user_to_ts
# 

# 
# START: reset_to_start
# 

def reset_user_to_start(user_id, is_dry_run):
    return _del_entries_for_query({"user_id": user_id}, is_dry_run)

def reset_all_users_to_start(is_dry_run):
    return _del_entries_for_query({}, is_dry_run)

def _del_entries_for_query(del_query, is_dry_run):
    """
        This is much easier. The steps are:
        - delete all analysis objects for this user
        - delete all pipeline states for this user
    """
    logging.info("About to delete %s analysis results" %
                    edb.get_analysis_timeseries_db().find(del_query).count())
    logging.info("About to delete entries with keys %s" 
        % edb.get_analysis_timeseries_db().find(del_query).distinct("metadata.key"))
    logging.info("About to delete %s pipeline states" % 
            (edb.get_pipeline_state_db().find(del_query).count()))

    if is_dry_run:
        logging.info("this is a dry run, returning from reset_user_to-start without modifying anything")
    else: 
        result = edb.get_analysis_timeseries_db().remove(del_query)
        logging.info("this is not a dry run, result of removing analysis objects = %s" % result)
        result = edb.get_pipeline_state_db().remove(del_query)
        logging.info("this is not a dry run, result of removing pipeline states = %s" % result)


# 
# END: reset_to_start
# 
