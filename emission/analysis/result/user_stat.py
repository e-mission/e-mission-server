# emission/analysis/result/user_stats.py

import logging
import pymongo
import arrow
from typing import Optional, Dict, Any
import emission.storage.timeseries.abstract_timeseries as esta
import emission.core.wrapper.user as ecwu

def get_last_call_timestamp(ts: esta.TimeSeries) -> Optional[int]:
    """
    Retrieves the last API call timestamp.

    :param ts: The time series object.
    :type ts: esta.TimeSeries
    :return: The last call timestamp or None if not found.
    :rtype: Optional[int]
    """
    last_call_ts = ts.get_first_value_for_field(
        key='stats/server_api_time',
        field='data.ts',
        sort_order=pymongo.DESCENDING
    )
    logging.debug(f"Last call timestamp: {last_call_ts}")
    return None if last_call_ts == -1 else last_call_ts


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

        logging.info(f"user_id type: {type(user_id)}")
        update_user_profile(user_id, update_data)

        logging.debug("User profile updated successfully.")

    except Exception as e:
        logging.error(f"Error in get_and_store_dependent_user_stats for user_id {user_id}: {e}")

def get_and_store_pipeline_independent_user_stats(user_id: str) -> None:
    """
    Aggregates and stores pipeline indepedent statistics into the user profile.
    These are statistics based on raw data, such as the last call, last push
    or last location received.

    :param user_id: The UUID of the user.
    :type user_id: str
    :return: None
    """

    try:
        logging.info(f"Starting get_and_store_pipeline_independent_user_stats for user_id: {user_id}")
        ts = esta.TimeSeries.get_time_series(user_id)
        last_call_ts = get_last_call_timestamp(ts)
        logging.info(f"Last call timestamp: {last_call_ts}")

        update_data = {
            "last_call_ts": last_call_ts
        }
        update_user_profile(user_id, update_data)

    except Exception as e:
        logging.error(f"Error in get_and_store_independent_user_stats for user_id {user_id}: {e}")
