# emission/analysis/result/user_stats.py

import logging
import pymongo
import arrow
from typing import Optional, Dict, Any
import emission.storage.timeseries.abstract_timeseries as esta
import emission.core.wrapper.user as ecwu

TIME_FORMAT = 'YYYY-MM-DD HH:mm:ss'

def count_trips(ts: esta.TimeSeries, key_list: list, extra_query_list: Optional[list] = None) -> int:
    """
    Counts the number of trips based on the provided query.

    :param ts: The time series object.
    :type ts: esta.TimeSeries
    :param key_list: List of keys to filter trips.
    :type key_list: list
    :param extra_query_list: Additional queries, defaults to None.
    :type extra_query_list: Optional[list], optional
    :return: The count of trips.
    :rtype: int
    """
    count = ts.find_entries_count(key_list=key_list, extra_query_list=extra_query_list)
    logging.debug(f"Counted {len(key_list)} trips with additional queries {extra_query_list}: {count}")
    return count


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


def get_and_store_user_stats(user_id: str, trip_key: str) -> None:
    """
    Aggregates and stores user statistics into the user profile.

    :param user_id: The UUID of the user.
    :type user_id: str
    :param trip_key: The key representing the trip data in the time series.
    :type trip_key: str
    :return: None
    """
    try:
        logging.info(f"Starting get_and_store_user_stats for user_id: {user_id}, trip_key: {trip_key}")

        ts = esta.TimeSeries.get_time_series(user_id)
        start_ts_result = ts.get_first_value_for_field(trip_key, "data.start_ts", pymongo.ASCENDING)
        start_ts = None if start_ts_result == -1 else start_ts_result

        end_ts_result = ts.get_first_value_for_field(trip_key, "data.end_ts", pymongo.DESCENDING)
        end_ts = None if end_ts_result == -1 else end_ts_result

        total_trips = count_trips(ts, key_list=["analysis/confirmed_trip"])
        labeled_trips = count_trips(
            ts,
            key_list=["analysis/confirmed_trip"],
            extra_query_list=[{'data.user_input': {'$ne': {}}}]
        )

        logging.info(f"Total trips: {total_trips}, Labeled trips: {labeled_trips}")
        logging.info(f"user_id type: {type(user_id)}")

        last_call_ts = get_last_call_timestamp(ts)
        logging.info(f"Last call timestamp: {last_call_ts}")

        update_data = {
            "pipeline_range": {
                "start_ts": start_ts,
                "end_ts": end_ts
            },
            "total_trips": total_trips,
            "labeled_trips": labeled_trips,
            "last_call_ts": last_call_ts
        }

        update_user_profile(user_id, update_data)

        logging.debug("User profile updated successfully.")

    except Exception as e:
        logging.error(f"Error in get_and_store_user_stats for user_id {user_id}: {e}")