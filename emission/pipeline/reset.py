from __future__ import unicode_literals
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import
from future import standard_library
standard_library.install_aliases()
from builtins import *
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
    if user_id is None:
        logging.info("user_id = None, skipping reset...")
        return

    # Find the place before the time
    try:
        last_cleaned_place = esdp.get_last_place_before(esda.CLEANED_PLACE_KEY, ts, user_id)
        logging.debug("last_cleaned_place = %s" % last_cleaned_place)
        if last_cleaned_place is None or last_cleaned_place.data.exit_ts is None:
            logging.info("Data collection for user %s stopped before reset time, early return" % user_id)
            return
    except ValueError as e:
        first_cleaned_place = esdp.get_first_place_entry(esda.CLEANED_PLACE_KEY, user_id)
        if first_cleaned_place is not None and first_cleaned_place.data.exit_ts > ts:
            logging.info("first_cleaned_place.exit = %s (%s), resetting to start" % 
                (first_cleaned_place.data.exit_ts,
                first_cleaned_place.data.exit_fmt_time))
            reset_user_to_start(user_id, is_dry_run)
            return
        else:
            raise

    last_raw_place_id = last_cleaned_place["data"]["raw_places"][-1]
    last_raw_place = esda.get_entry(esda.RAW_PLACE_KEY, last_raw_place_id)
    logging.debug("last_raw_place = %s" % last_raw_place)

    # Reason for using first_raw_place is
    # https://github.com/e-mission/e-mission-server/issues/333#issuecomment-312735236
    first_raw_place_id = last_cleaned_place["data"]["raw_places"][0]
    first_raw_place = esda.get_entry(esda.RAW_PLACE_KEY, first_raw_place_id)
    logging.debug("first_raw_place = %s" % first_raw_place)

    last_place_enter_ts = first_raw_place.data.enter_ts
    logging.debug("last_place_enter_ts = %s" % last_place_enter_ts)

    reset_ts = last_place_enter_ts
    logging.debug("reset_ts = %s" % last_place_enter_ts)

    # clear all analysis results after it
    del_objects_after(user_id, reset_ts, is_dry_run)

    # open the raw and cleaned places
    reset_last_place(last_cleaned_place, is_dry_run)
    reset_last_place(last_raw_place, is_dry_run)

    # reset pipeline states to its enter_ts
    reset_pipeline_state(user_id, reset_ts, is_dry_run)

def del_objects_after(user_id, reset_ts, is_dry_run):
    del_query = {}
    # handle the user
    del_query.update({"user_id": user_id})

    date_query_list = []
    # handle all trip-like entries
    date_query_list.append({"data.start_ts": {"$gt": reset_ts}})
    # handle all place-like entries
    date_query_list.append({"data.enter_ts": {"$gt": reset_ts}})
    # handle all reconstructed points
    date_query_list.append({"data.ts": {"$gt": reset_ts}})

    del_query.update({"$or": date_query_list})
    logging.debug("After all updates, del_query = %s" % del_query)
    logging.info("About to delete %d entries" 
        % edb.get_analysis_timeseries_db().count_documents(del_query))
    logging.info("About to delete entries with keys %s" 
        % edb.get_analysis_timeseries_db().find(del_query).distinct("metadata.key"))

    if is_dry_run:
        logging.info("this is a dry-run, returning from del_objects_after without modifying anything")
    else:
        result = edb.get_analysis_timeseries_db().remove(del_query)
        logging.info("this is not a dry-run, result of deleting analysis entries is %s" % result)

def reset_last_place(last_place, is_dry_run):
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
    reset_query = {'$unset' : {"data.exit_ts": "",
                               "data.exit_local_dt": "",
                               "data.exit_fmt_time": "",
                               "data.starting_trip": "",
                               "data.duration": ""
                               }}

    if last_place.metadata.key == esda.CLEANED_PLACE_KEY:
        reset_query.update({"$set": {"data.raw_places": []}})

    logging.debug("reset_query = %s" % reset_query)

    result = edb.get_analysis_timeseries_db().update(match_query, reset_query)
    logging.debug("this is not a dry run, result of update in reset_last_place = %s" % result)

    logging.debug("after update, entry is %s" %
                  edb.get_analysis_timeseries_db().find_one(match_query))

def reset_pipeline_state(user_id, reset_ts, is_dry_run):
    stages_list = ecwp.PipelineStages

    # Fuzz the TRIP_SEGMENTATION stage 5 mins because of
    # https://github.com/e-mission/e-mission-server/issues/333#issuecomment-312730217
    FUZZ_FACTOR = 5 * 60
    if reset_ts is None:
        logging.info("reset_ts = %s, returning from reset_pipeline_state without modifying anything" % None)
        return
        
    trip_seg_reset_pipeline_query = {'user_id': user_id,
                                     'last_processed_ts': {'$ne': None},
    # only reset entries that are after the reset_ts
    # https://github.com/e-mission/e-mission-server/issues/333#issuecomment-312958309
                                     'last_processed_ts': {'$gt': reset_ts + FUZZ_FACTOR},
                                     'pipeline_stage': ecwp.PipelineStages.TRIP_SEGMENTATION.value}
    trip_seg_update_pipeline_query = {'$set': {'last_processed_ts': reset_ts + FUZZ_FACTOR}}
    logging.debug("trip_seg_reset_pipeline_query = %s" % trip_seg_reset_pipeline_query)
    logging.debug("trip_seg_update_pipeline_query = %s" % trip_seg_update_pipeline_query)
    logging.info("resetting %s trip_seg_pipeline states for user %s to %s" % 
            (edb.get_pipeline_state_db().count_documents(trip_seg_reset_pipeline_query),
            user_id, reset_ts + FUZZ_FACTOR))

    # Don't fuzz the others because of 
    # https://github.com/e-mission/e-mission-server/issues/333#issuecomment-312917119
    reset_pipeline_query = {'user_id': user_id,
                            'last_processed_ts': {'$ne': None},
    # only reset entries that are after the reset_ts
    # https://github.com/e-mission/e-mission-server/issues/333#issuecomment-312958309
                            'last_processed_ts': {'$gt': reset_ts},
                            'pipeline_stage': {'$ne': ecwp.PipelineStages.TRIP_SEGMENTATION.value}}
    update_pipeline_query = {'$set': {'last_processed_ts': reset_ts}}
    logging.debug("reset_pipeline_query = %s" % reset_pipeline_query)
    logging.debug("update_pipeline_query = %s" % update_pipeline_query)
    logging.info("out of %s total, resetting %s pipeline states for user %s to %s" % 
            (edb.get_pipeline_state_db().count_documents({'user_id': user_id}),
            edb.get_pipeline_state_db().count_documents(reset_pipeline_query),
            user_id, reset_ts))

    if is_dry_run:
        logging.info("this is a dry run, returning from reset_pipeline_state without modifying anything")
    else:
        result = edb.get_pipeline_state_db().update(
                    trip_seg_reset_pipeline_query, trip_seg_update_pipeline_query,
                    upsert=False)
        logging.debug("this is not a dry run, result of updating trip_segmentation stage in reset_pipeline_state = %s" % result)

        result = edb.get_pipeline_state_db().update(
                    reset_pipeline_query, update_pipeline_query,
                    upsert=False, multi=True)
        logging.debug("this is not a dry run, result of updating all other stages in reset_pipeline_state = %s" % result)
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
                    edb.get_analysis_timeseries_db().count_documents(del_query))
    logging.info("About to delete entries with keys %s" 
        % edb.get_analysis_timeseries_db().find(del_query).distinct("metadata.key"))
    logging.info("About to delete %s pipeline states" % 
            (edb.get_pipeline_state_db().count_documents(del_query)))

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
