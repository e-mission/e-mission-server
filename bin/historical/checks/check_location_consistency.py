
import pymongo
import logging
import arrow
import argparse
import time
import pandas as pd

import emission.core.get_database as edb
import emission.core.wrapper.user as ecwu
import emission.core.wrapper.pipelinestate as ecwp

TEN_MINUTES = 10 * 60

def query_with_retry(timeseries_db, aggregate_ops):
    # this is initialized to 1 so that we will wait for at least one minute
    # even on the first retry. If we do get an OOM, there's no point in
    # retrying after 0 secs
    retry_count = 1
    while retry_count < 6:
        try:
            return list(timeseries_db.aggregate(aggregate_ops))
        except pymongo.errors.OperationFailure as e:
            if e.code == 39:
                print(f"{arrow.now()} On {retry_count=}, got OOM {e.details}, sleeping for {retry_count * 60} secs before retrying")
                retry_count += 1
                time.sleep(retry_count * 60)
            else:
                logging.exception(e)
                raise
    assert retry_count == 6
    raise TimeoutError(f"Too many retries {retry_count=}, giving up for now")

def get_recent_entry(user_id: str, key: str, name: str, timeseries_db: pymongo.collection):
    # we are going to use edb directly since this is a one-off throwaway script
    # and using a direct query allows us to filter instead of reading
    # everything from the database. Although given the index issues, maybe we
    # will end up reading everything into memory anyway
    query = {"user_id": user_id, "metadata.key": key}
    if name is not None:
        query["data.name"] = name

    result_list = query_with_retry(timeseries_db,
    [
        {"$match": query},
        {"$sort": {"data.ts": -1}},
        {"$limit": 1}
    ])
    # again, we should be using "data.ts" instead of "metadata.write_ts", but 
    # "metadata.write_ts" is guaranteed to exist, and we don't want to miss entries
    # and this is backfilling for compatibility anyway
    return -1 if len(result_list) == 0 else result_list[0]["metadata"]["write_ts"]

def check_timeline_consistency(profile_list = None):
    if profile_list is None:
        profile_list = list(edb.get_profile_db().find())

    for profile in profile_list:
        pipeline_states = pd.json_normalize(edb.get_pipeline_state_db().find({"user_id": profile['user_id']}))[["pipeline_stage", "last_processed_ts"]].dropna().set_index("pipeline_stage").sort_index()
        pipeline_states = pipeline_states.dropna()
        pipeline_states['formatted_stage'] = [ecwp.PipelineStages(s) for s in pipeline_states.index.to_list()]
        MOST_COMMON_FAILURE = [ecwp.PipelineStages.USER_INPUT_MATCH_INCOMING, ecwp.PipelineStages.TRIP_MODEL]
        pipeline_states = pipeline_states.query("formatted_stage not in @MOST_COMMON_FAILURE")
        pipeline_states['formatted_processed_ts'] = pipeline_states.last_processed_ts.apply(lambda t: arrow.get(t))
        print(f"For user {profile['user_id']}, last processed ts are: {pipeline_states[['formatted_stage', 'formatted_processed_ts']]}")
        processed_diffs = pipeline_states.last_processed_ts.diff()
        print(f"For user {profile['user_id']}, diffs in last_ts_processed between stages are: {processed_diffs.to_list()}")
        if processed_diffs.max() > TEN_MINUTES:
            print("-" * 10, f"ERROR: User {profile['user_id']} has a gap in pipeline processing greater than TEN_MINUTES.", "-" * 10)
        

def check_location_consistency(profile_list = None):
    timeseries_db = edb.get_timeseries_db()
    usercache_db = edb.get_usercache_db()
    if profile_list is None:
        profile_list = list(edb.get_profile_db().find())

    for profile in profile_list:
        last_location_timeseries = get_recent_entry(profile['user_id'], "background/location", None, timeseries_db)
        last_location_usercache = get_recent_entry(profile['user_id'], "background/location", None, usercache_db)
        last_location = max(last_location_timeseries, last_location_usercache)
        last_filtered_location_timeseries = get_recent_entry(profile['user_id'], "background/filtered_location", None, timeseries_db)
        last_filtered_location_usercache = get_recent_entry(profile['user_id'], "background/filtered_location", None, usercache_db)
        last_filtered_location = max(last_filtered_location_timeseries, last_filtered_location_usercache)
        last_loc_in_profile = profile['last_location_ts']
        sensor_loc_diff = abs(last_location - last_filtered_location)
        sensor_loc_check = sensor_loc_diff > TEN_MINUTES
        profile_loc_diff = abs(last_loc_in_profile - last_location)
        profile_loc_check = profile_loc_diff > TEN_MINUTES
        print(f"User {profile['user_id']} has location data: ")
        print(f"last_location_timeseries = {arrow.get(last_location_timeseries)}, "
            f"last_location_usercache = {arrow.get(last_location_usercache)}, "
            f"-> max last_location = {arrow.get(last_location)}, ")
        print(f"last_filtered_location_timeseries = {arrow.get(last_filtered_location_timeseries)}, "
            f"last_filtered_location_usercache = {arrow.get(last_filtered_location_usercache)}, "
            f"-> max last_filtered_location = {arrow.get(last_filtered_location)}, ")
        print(f"last_loc_in_profile = {arrow.get(last_loc_in_profile)}")
        if  sensor_loc_check or profile_loc_check:
            print("-" * 10, f"FAILED: User {profile['user_id']} {sensor_loc_diff=}, {sensor_loc_check=}, {profile_loc_diff=}, {profile_loc_check=}", "-" * 10)
        else: 
            print("-" * 10, f"SUCCESS: User {profile['user_id']} {sensor_loc_diff=}, {sensor_loc_check=}, {profile_loc_diff=}, {profile_loc_check=}", "-" * 10)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    run_group = parser.add_mutually_exclusive_group(required=True)
    run_group.add_argument("-p", "--processed", help="check the consistency of the processed timestamp ", action="store_true")
    run_group.add_argument("-l", "--location", help="check location timestamp consistency", action="store_true")

    run_group = parser.add_mutually_exclusive_group(required=True)
    run_group.add_argument("-s", "--single", help="run for the single deployment configured in DB_HOST", action="store_true")
    run_group.add_argument("-a", "--all", help="run against all currently active deployments", action="store_true")

    args = parser.parse_args()

    if args.location:
        fn_to_run = check_location_consistency
    if args.processed:
        fn_to_run = check_timeline_consistency

    if args.single:
        # from uuid import UUID
        # fn_to_run([edb.get_profile_db().find_one({"user_id": UUID("redacted")})])
        fn_to_run()
    if args.all:
        # this currently downloads the deployments on module load
        # until that is refactored, importing this at the top will
        # have us download configs even when running against the current single
        # deployment. Importing in here lets us only "git clone" when really needed
        from bin.federation import run_on_all_deployments
        run_on_all_deployments(fn_to_run)
