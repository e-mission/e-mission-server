import emission.core.get_database as edb

import pymongo
import logging
import arrow
import datetime
import argparse
import time

import collections.abc as cabc

import emission.core.get_database as edb
import emission.core.wrapper.user as ecwu

CALL_SUMMARY_MAP = \
    {"last_call_ts": None,
    "last_sync_ts": "/usercache/get",
    "last_put_ts": "/usercache/put",
    "last_diary_fetch_ts": "/pipeline/get_range_ts"}

UPLOAD_SUMMARY_MAP = \
        {"last_location_ts": "background/location",
        "last_phone_data_ts": "background/battery"}


def update_user_profile(user_id: str, data: dict[str, any]) -> None:
    """
    Updates the user profile with the provided data.

    :param user_id: The UUID of the user.
    :type user_id: str
    :param data: The data to update in the user profile.
    :type data: Dict[str, Any]
    :return: None
    """
    if len(data) == 0:
        logging.info(f"Data blank, skipping update")
        return
    user = ecwu.User.fromUUID(user_id)
    user.update(data)
    logging.debug(f"User profile updated with data: {data}")
    logging.debug(f"New profile: {user.getProfile()}")

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
    raise TimeoutError("Too many retries {retry_count=}, giving up for now")

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

def fill_if_missing(profile: dict, fill_fn: cabc.Callable, field_arg_map: dict) -> dict:
    update_data = {}
    for field, fill_arg in field_arg_map.items():
        if field not in profile or profile[field] == -1:
            print(f"Filling missing {field=} with {fill_arg=} ")
            try:
                update_data[field] = fill_fn(fill_arg)
            except TimeoutError as e:
                print(f"Too many retries, skipping {field=} with {fill_arg=}")
    return update_data

def populate_call_summary(profile: dict, timeseries_db: pymongo.collection):
    print(f"{arrow.now()}: Populating call summmary for {profile['user_id']}")
    get_call_ts = lambda name: get_recent_entry(profile["user_id"],
        "stats/server_api_time", "POST_"+name if name is not None else None, timeseries_db)

    # It is not strictly accurate to use `/usercache/get` for `last_sync_ts`
    # because a `put` from the same sync may have come in after the get, but
    # that will be reflected in `last_put_ts` and this is backfilling for
    # backwards compatibility anyway, so some fuzziness is OK
    update_data = fill_if_missing(profile, get_call_ts, CALL_SUMMARY_MAP)
    update_user_profile(profile["user_id"], update_data)

def populate_upload_summary(profile: dict, timeseries_db: pymongo.collection):
    print(f"{arrow.now()}: Populating upload summmary for {profile['user_id']}")
    get_upload_ts = lambda key: get_recent_entry(profile["user_id"], key,
        None, timeseries_db)

    # There could be other background data that has been sent up after the battery, e.g. 
    # background/motion_activity, but querying for it will be complicated
    # because we would need to query for `background/*` and I am not sure
    # how DocumentDB will do with regex. This is just backfilling for
    # backwards compat anyway, so some fuzziness is OK
    update_data = fill_if_missing(profile, get_upload_ts, UPLOAD_SUMMARY_MAP)
    update_user_profile(profile["user_id"], update_data)

def get_create_update_data(profile: dict, timeseries_db: pymongo.collection):
    query = {
        "user_id": profile["user_id"],
        "metadata.key": "stats/server_api_time",
        "data.name": "POST_/datastreams/find_entries/timestamp"
    }
    first_create_entry = query_with_retry(timeseries_db, [
                {"$match": query},
                {"$sort": {"data.ts": 1}},
                {"$limit": 1}
    ])
    raw_create_ts = -1 if len(first_create_entry) == 0 else first_create_entry[0]["data"]["ts"]
    update_data = {"create_ts": -1 if raw_create_ts == -1 else datetime.datetime.fromtimestamp(raw_create_ts)}
    return update_data

def populate_create_ts(profile: dict, timeseries_db: pymongo.collection):
    print(f"{arrow.now()} Populating create ts for {profile['user_id']}")
    create_update_data = None
    # The obvious approach would be to use `/profile/create`, but it is not
    # linked with a UUID, so we use the first `/datastreams/find_entries/timestamp`
    # which occurs right after the profile creation to retrieve the demographic survey
    # https://github.com/e-mission/e-mission-docs/issues/1111#issuecomment-2655738722
    if 'create_ts' not in profile:
        try:
            create_update_data = get_create_update_data(profile, timeseries_db)
            update_user_profile(profile["user_id"], create_update_data)
        except TimeoutError as e:
            print(f"Too many retries, skipping create_ts setting in the profile DB")

    uuid_entry = edb.get_uuid_db().find_one({"uuid": profile["user_id"]})
    if uuid_entry is None:
        print(f"No entry in the UUID DB found for {profile['user_id']}, ignoring...")
    elif "create_ts" not in uuid_entry:
        # in the common case, if the profile doesn't have a create_ts, the UUID
        # entry will not either. And both of them should have the `create_ts`
        # set to the same value. So let's reuse where we can instead of
        # querying the database again
        if create_update_data is None:
            # this may be None even if there is valid entry in the timeseries
            # because the profile already had a `create_ts` field so we didn't
            # read anything
            try:
                create_update_data = get_create_update_data(profile, timeseries_db)
                edb.get_uuid_db().update_one({"uuid": profile["user_id"]},
                    {"$set": create_update_data})
            except TimeoutError as e:
                print(f"Too many retries, skipping create_ts setting in the UUID DB")
        else:
            assert create_update_data is not None
            edb.get_uuid_db().update_one({"uuid": profile["user_id"]},
                {"$set": create_update_data})

def populate_profiles(profile_list = None):
    timeseries_db = edb.get_timeseries_db()
    if profile_list is None:
        profile_list = list(edb.get_profile_db().find())

    for profile in profile_list:
        populate_call_summary(profile, timeseries_db)
        populate_upload_summary(profile, timeseries_db)
        populate_create_ts(profile, timeseries_db)

def list_profile_completion(profile_list = None):
    if profile_list is None:
        profile_list = list(edb.get_profile_db().find())
    ALL_FIELDS = list(CALL_SUMMARY_MAP.keys()) + list(UPLOAD_SUMMARY_MAP.keys()) + ["create_ts"]

    # profile_populate_summary = pps
    pps = {"total_profiles": len(profile_list), "all_found": 0,
        "field_not_found": 0, "value_not_found": 0}
    for profile in profile_list:
        field_not_found_list = [f not in profile for f in ALL_FIELDS]
        value_not_found_list = [f in profile and profile[f] == -1 for f in ALL_FIELDS]
        if any(field_not_found_list):
            print(f"{field_not_found_list=} for {profile['user_id']}, {profile=}")
        if any(value_not_found_list):
            print(f"{value_not_found_list=} for {profile['user_id']}, {profile=}")
        pps["field_not_found"] += int(any(field_not_found_list))
        pps["value_not_found"] += int(any(value_not_found_list))
        pps["all_found"] += int(not any(field_not_found_list) and not any(value_not_found_list))
    print(pps)
    return pps

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    op_group = parser.add_mutually_exclusive_group(required=True)
    op_group.add_argument("-l", "--list", help="list current status of profiles", action="store_true")
    op_group.add_argument("-p", "--populate", help="populate profiles", action="store_true")

    run_group = parser.add_mutually_exclusive_group(required=True)
    run_group.add_argument("-s", "--single", help="run for the single deployment configured in DB_HOST", action="store_true")
    run_group.add_argument("-a", "--all", help="run against all currently active deployments", action="store_true")

    args = parser.parse_args()

    if args.list:
        fn_to_run = list_profile_completion
    if args.populate:
        fn_to_run = populate_profiles

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
