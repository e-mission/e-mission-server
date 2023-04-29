from __future__ import unicode_literals
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import
from future import standard_library
standard_library.install_aliases()
from builtins import *
import logging
import pandas as pd
import arrow

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
        last_confirmed_place = esdp.get_last_place_before(esda.CONFIRMED_PLACE_KEY, ts, user_id)
        logging.debug("last_confirmed_place = %s" % last_confirmed_place)
        if last_confirmed_place is None:
            logging.info("last_confirmed_place is None, resetting to start and early return")
            reset_user_to_start(user_id, is_dry_run)
            return
    except ValueError as e:
        first_confirmed_place = esdp.get_first_place_entry(esda.CONFIRMED_PLACE_KEY, user_id)
        if first_confirmed_place is not None and first_confirmed_place.data.exit_ts > ts:
            logging.info("first_confirmed_place.exit = %s (%s), resetting to start" %
                (first_confirmed_place.data.exit_ts,
                first_confirmed_place.data.exit_fmt_time))
            reset_user_to_start(user_id, is_dry_run)
            return
        else:
            raise

    reset_ts, last_cleaned_place, last_raw_place = get_reset_ts(user_id, last_confirmed_place, is_dry_run)

    # clear all analysis results after it
    del_objects_after(user_id, reset_ts, is_dry_run)

    # open the raw and cleaned places
    reset_last_place(last_confirmed_place, is_dry_run)
    reset_last_place(last_cleaned_place, is_dry_run)
    reset_last_place(last_raw_place, is_dry_run)

    # reset pipeline states to its enter_ts
    reset_pipeline_state(user_id, reset_ts, is_dry_run)

    # reset any curr_run_ts
    reset_curr_run_state(user_id, is_dry_run)

def get_reset_ts(user_id, last_confirmed_place, is_dry_run):
    assert last_confirmed_place is not None, "last_confirmed_place = %s" % last_confirmed_place

    last_cleaned_place_id = last_confirmed_place["data"]["cleaned_place"]
    last_cleaned_place = esda.get_entry(esda.CLEANED_PLACE_KEY, last_cleaned_place_id)

    # TODO: Remove me in 2023
    # Historically, when we unset the entries for the last cleaned place during
    # a reset, we set the raw place array to empty. if the next run of the
    # pipeline also fails, then the last cleaned place will not have any raw
    # places associated with it. In that case, we find the matching raw place
    # and fill it in
    # Hack to deal with this historical fact
    if len(last_cleaned_place["data"]["raw_places"]) == 0:
        ending_trip = esda.get_entry(esda.CLEANED_TRIP_KEY, last_cleaned_place["data"]["ending_trip"])
        ending_raw_trip = esda.get_entry(esda.RAW_TRIP_KEY, ending_trip["data"]["raw_trip"])
        raw_place_id = ending_raw_trip["data"]["end_place"]
        last_cleaned_place["data"]["raw_places"] = [raw_place_id]
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
    return (reset_ts, last_cleaned_place, last_raw_place)

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
        result = edb.get_analysis_timeseries_db().delete_many(del_query)
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

    result = edb.get_analysis_timeseries_db().update_one(match_query, reset_query)
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

    stages_with_fuzz = [ecwp.PipelineStages.TRIP_SEGMENTATION.value,
                        ecwp.PipelineStages.LABEL_INFERENCE.value,
                        ecwp.PipelineStages.EXPECTATION_POPULATION.value,
                        ecwp.PipelineStages.CREATE_CONFIRMED_OBJECTS.value,
                        ecwp.PipelineStages.CREATE_COMPOSITE_OBJECTS.value]
        
    trip_seg_reset_pipeline_query = {'user_id': user_id,
                                     'last_processed_ts': {'$ne': None},
    # only reset entries that are after the reset_ts
    # https://github.com/e-mission/e-mission-server/issues/333#issuecomment-312958309
                                     'last_processed_ts': {'$gt': reset_ts + FUZZ_FACTOR},
                                     'pipeline_stage': {'$in': stages_with_fuzz}}
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
                            'pipeline_stage': {'$nin': stages_with_fuzz}}
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
        result = edb.get_pipeline_state_db().update_many(
                    trip_seg_reset_pipeline_query, trip_seg_update_pipeline_query,
                    upsert=False)
        logging.debug("this is not a dry run, result of updating trip_segmentation stage in reset_pipeline_state = %s" % result)

        result = edb.get_pipeline_state_db().update_many(
                    reset_pipeline_query, update_pipeline_query,
                    upsert=False)
        logging.debug("this is not a dry run, result of updating all other stages in reset_pipeline_state = %s" % result)


def reset_curr_run_state(user_id, is_dry_run):
    reset_curr_run_ts_query = {"user_id": user_id, "curr_run_ts": {"$ne": None}}
    reset_curr_run_ts_update = {"$set": {"curr_run_ts": None}}
    logging.debug("reset_curr_run_ts_query = %s" % reset_curr_run_ts_query)
    logging.debug("reset_curr_run_ts_update = %s" % reset_curr_run_ts_update)
    if is_dry_run:
        logging.info("this is a dry run, returning from reset_curr_run_state without modifying anything")
    else:
        result = edb.get_pipeline_state_db().update_many(
                    reset_curr_run_ts_query, reset_curr_run_ts_update)
        logging.debug("this is not a dry run, result of removing any curr_run_ts entries = %s" % result)
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
        result = edb.get_analysis_timeseries_db().delete_many(del_query)
        logging.info("this is not a dry run, result of removing analysis objects = %s" % result)
        result = edb.get_pipeline_state_db().delete_many(del_query)
        logging.info("this is not a dry run, result of removing pipeline states = %s" % result)


# 
# END: reset_to_start
# 

#
# START: auto_reset
#

THREE_HOURS_IN_SECS = 60 * 60 * 3

def fill_and_print(all_invalid_states):
    all_invalid_states['curr_run_fmt_time'] = all_invalid_states.curr_run_ts.apply(lambda ts: arrow.get(ts).format("YYYY-MM-DDTHH:mm:ssZZ"))
    all_invalid_states['pipeline_stage_name'] = all_invalid_states.pipeline_stage.apply(lambda ps: ecwp.PipelineStages(ps).name)
    logging.debug("----- All invalid states --------")
    logging.debug(all_invalid_states[['user_id', 'pipeline_stage', 'curr_run_fmt_time']])

def split_single_and_multi(all_invalid_states):
    all_invalid_states_grouped = all_invalid_states.groupby(by="user_id")
    multiple_state_uuids = all_invalid_states_grouped.count().query('curr_run_ts > 1').index.to_list()
    single_state_uuids = all_invalid_states_grouped.count().query('curr_run_ts == 1').index.to_list()
    logging.debug("Found multiple states for UUIDs %s" % multiple_state_uuids)
    logging.debug("Found single states for UUIDs %s" % single_state_uuids)
    multiple_state_groups = all_invalid_states_grouped.filter(lambda x: x.name in multiple_state_uuids)
    single_state_groups = all_invalid_states_grouped.filter(lambda x: x.name in single_state_uuids)
    return multiple_state_groups, single_state_groups

def get_single_state_resets(single_state_groups):
    # print(single_state_groups[["user_id", "pipeline_stage_name", "curr_run_fmt_time"]])
    reset_ts = single_state_groups.copy()
    reset_ts["reset_ts"] = single_state_groups.curr_run_ts.apply(lambda ts: ts - THREE_HOURS_IN_SECS)
    cols_to_drop = [c for c in reset_ts.columns if c not in ["user_id", "reset_ts"]]
    # print(cols_to_drop)
    reset_ts.drop(cols_to_drop, axis=1, inplace=True)
    # print(reset_ts.columns)
    return reset_ts
        # epr.reset_user_to_ts(user_id, three_hours_before, dry_run)

def get_multi_state_resets(multi_state_groups):
    earliest_invalid_state_per_user = multi_state_groups.groupby(by="user_id").min()
    print(earliest_invalid_state_per_user.columns)
    reset_ts = earliest_invalid_state_per_user.apply(lambda g: g.curr_run_ts - THREE_HOURS_IN_SECS, axis=1).reset_index()
    # print(reset_ts.columns)
    reset_ts.rename(columns={0: "reset_ts"}, inplace=True)
    # print(reset_ts)
    return reset_ts

def get_all_resets(all_invalid_states):
    fill_and_print(all_invalid_states)
    multiple_state_groups, single_state_groups = split_single_and_multi(all_invalid_states)
    reset_ts_single = get_single_state_resets(single_state_groups)
    reset_ts_multi = get_multi_state_resets(multiple_state_groups)
    reset_ts = pd.concat([reset_ts_single, reset_ts_multi])
    reset_ts["reset_ts_fmt"] = reset_ts.reset_ts.apply(lambda t: arrow.get(t))
    print(reset_ts)
    return reset_ts

def auto_reset(dry_run, only_calc):
    # Only read all states that are not for `OUTPUT_GEN` since we are not going to reset that state
    # Also only read states which have been running for more than three hours
    # If we are running the pipeline every hour, then having a run_ts that is
    # more than three hours old indicates that it is likely invalid
    three_hours_ago = arrow.utcnow().shift(hours=-3).int_timestamp
    all_invalid_states = pd.json_normalize(list(edb.get_pipeline_state_db().find({"$and": [
        {"curr_run_ts": {"$lt": three_hours_ago}},
        {"pipeline_stage": {"$ne": 9}}]})))
    if len(all_invalid_states) == 0:
        logging.info("No invalid states found, returning early")
        return
    reset_ts = get_all_resets(all_invalid_states)
    if only_calc:
        print("finished calculating values, early return")
        return

    for index, invalid_state in reset_ts.iterrows():
        print(f"Resetting {invalid_state['user_id']} to {arrow.get(invalid_state['reset_ts'])}")
        reset_user_to_ts(invalid_state['user_id'], invalid_state['reset_ts'], dry_run)

#
# END: auto_reset
#
