import logging
import pymongo

import emission.storage.timeseries.timequery as estt

import emission.core.wrapper.entry as ecwe
import emission.storage.timeseries.abstract_timeseries as esta

RAW_TRIP_KEY = "segmentation/raw_trip"
RAW_PLACE_KEY = "segmentation/raw_place"
RAW_SECTION_KEY = "segmentation/raw_section"
RAW_STOP_KEY = "segmentation/raw_stop"
CLEANED_TRIP_KEY = "analysis/cleaned_trip"
CLEANED_PLACE_KEY = "analysis/cleaned_place"
CLEANED_SECTION_KEY = "analysis/cleaned_section"
CLEANED_STOP_KEY = "analysis/cleaned_stop"
CLEANED_LOCATION_KEY = "analysis/recreated_location"
METRICS_DAILY_USER_COUNT = "metrics/daily_user_count"
METRICS_DAILY_MEAN_COUNT = "metrics/daily_mean_count"
METRICS_DAILY_USER_DISTANCE = "metrics/daily_user_distance"
METRICS_DAILY_MEAN_DISTANCE = "metrics/daily_mean_distance"
METRICS_DAILY_USER_DURATION = "metrics/daily_user_duration"
METRICS_DAILY_MEAN_DURATION = "metrics/daily_mean_duration"
METRICS_DAILY_USER_MEDIAN_SPEED = "metrics/daily_user_median_speed"
METRICS_DAILY_MEAN_MEDIAN_SPEED = "metrics/daily_mean_median_speed"

# General methods

def get_object(key, object_id):
    return get_entry(key, object_id).data

def get_entry(key, object_id):
    return esta.TimeSeries.get_aggregate_time_series().get_entry_from_id(
        key, object_id)

def get_objects(key, user_id, time_query, geo_query=None):
    return [entry.data for entry in
            get_entries(key, user_id=user_id, time_query=time_query,
                        geo_query=geo_query)]

def get_entries(key, user_id, time_query, geo_query=None,
                extra_query_list=None):
    ts = get_timeseries_for_user(user_id)
    doc_cursor = ts.find_entries([key], time_query, geo_query, extra_query_list)
    # TODO: Fix "TripIterator" and return it instead of this list
    curr_entry_list = [ecwe.Entry(doc) for doc in doc_cursor]
    logging.debug("Returning entry with length %d result" % len(curr_entry_list))
    return curr_entry_list

def get_data_df(key, user_id, time_query, geo_query=None,
                extra_query_list=None):
    ts = get_timeseries_for_user(user_id)
    data_df = ts.get_data_df(key, time_query,
                     geo_query, extra_query_list)
    logging.debug("Returning entry with length %d result" % len(data_df))
    return data_df

def get_timeseries_for_user(user_id):
    if user_id is not None:
        ts = esta.TimeSeries.get_time_series(user_id)
    else:
        ts = esta.TimeSeries.get_aggregate_time_series()
    logging.debug("for user %s, returning timeseries %s" % (user_id, ts))
    return ts

# Object-specific associations

def get_time_query_for_trip_like(key, trip_like_id):
    """
    Returns the query that returns all the points associated with this
    trip-like (raw trip, cleaned trip, raw section) 
    """
    trip = get_object(key, trip_like_id)
    return estt.TimeQuery("data.ts", trip.start_ts, trip.end_ts)

