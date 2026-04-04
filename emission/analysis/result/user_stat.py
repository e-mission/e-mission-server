# emission/analysis/result/user_stats.py

import logging
import pymongo
import arrow
from typing import Optional, Dict, Any
import emission.storage.timeseries.abstract_timeseries as esta
import emission.core.wrapper.user as ecwu

def update_upload_timestamp(user_id: str, stat_name: str, ts: float) -> None:
    """
    Updates the upload timestamps in the profile

    :param user_id: The user's UUID
    :type user_id: str
    :param stat_name: The field name that is updated
    :type stat_name: str
    :param ts: The timestamp to store (may not always be 'now')
    :type ts: float
    :return: None
    """
    update_data = {
        stat_name: ts
    }
    update_user_profile(user_id, update_data)

def update_last_call_timestamp(user_id: str, call_path: str) -> Optional[int]:
    """
    Updates the user profile with server call starts

    :param user_id: The user's UUID
    :type user_id: str
    :param call_path: Can be used to store different call stats
    :type ts: str
    :return: None
    """
    logging.debug(f"update_last_call_timestamp called with: {user_id=}, {call_path=}")
    now = arrow.now().timestamp()
    update_data = {
        "last_call_ts": now
    }
    if "usercache" in call_path:
        update_data["last_sync_ts"] = now
    if "usercache/put" in call_path:
        update_data["last_put_ts"] = now
    if call_path == "/pipeline/get_range_ts":
        update_data["last_diary_fetch_ts"] = now
    update_user_profile(user_id, update_data)

def update_user_profile(user_id: str, data: Dict[str, Any]) -> None:
    """
    Updates the user profile with the provided data.

    :param user_id: The UUID of the user.
    :type user_id: str
    :param data: The data to update in the user profile.
    :type data: Dict[str, Any]
    :return: None
    """
    user = ecwu.User.fromUUID(user_id)
    user.update(data)
    logging.debug(f"User profile updated with data: {data}")
    logging.debug(f"New profile: {user.getProfile()}")


def get_pipeline_dependent_user_query(user_id: str, trip_key: str) -> Dict[str,str]:
    """
    Aggregates and stores pipeline dependent into the user profile.
    These are statistics based on analysed data such as trips or labels.

    :param user_id: The UUID of the user.
    :type user_id: str
    :param trip_key: The key representing the trip data in the time series.
    :type trip_key: str
    :return: the query to update the profile with this information; refactored
        for reuse while resetting the pipeline
    """
    ts = esta.TimeSeries.get_time_series(user_id)
    start_ts_result = ts.get_first_value_for_field(trip_key, "data.start_ts", pymongo.ASCENDING)
    start_ts = None if start_ts_result == -1 else start_ts_result

    end_ts_result = ts.get_first_value_for_field(trip_key, "data.end_ts", pymongo.DESCENDING)
    end_ts = None if end_ts_result == -1 else end_ts_result

    total_trips = ts.find_entries_count(key_list=["analysis/confirmed_trip"])
    labeled_trips = ts.find_entries_count(
        key_list=["analysis/confirmed_trip"],
        extra_query_list=[{'data.user_input': {'$ne': {}}}]
    )

    logging.info(f"Total trips: {total_trips}, Labeled trips: {labeled_trips}")
    update_data = {
        "pipeline_range": {
            "start_ts": start_ts,
            "end_ts": end_ts
        },
        "total_trips": total_trips,
        "labeled_trips": labeled_trips,
    }

    return update_data

def get_and_store_pipeline_dependent_user_stats(user_id: str, trip_key: str) -> None:
    """
    Aggregates and stores pipeline dependent into the user profile.
    These are statistics based on analysed data such as trips or labels.

    :param user_id: The UUID of the user.
    :type user_id: str
    :param trip_key: The key representing the trip data in the time series.
    :type trip_key: str
    :return: None
    """
    try:
        logging.info(f"Starting get_and_store_pipeline_dependent_user_stats for user_id: {user_id}, trip_key: {trip_key}")

        update_data = get_pipeline_dependent_user_query(user_id, trip_key)
        logging.info(f"user_id type: {type(user_id)}")
        update_user_profile(user_id, update_data)

        logging.debug("User profile updated successfully.")

    except Exception as e:
        logging.error(f"Error in get_and_store_dependent_user_stats for user_id {user_id}: {e}")

