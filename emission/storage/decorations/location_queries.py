from __future__ import print_function
import logging
import datetime as pydt
import pandas as pd
import time
import pymongo
import attrdict as ad
from enum import Enum

import emission.core.get_database as edb

def get_uuid_list():
    return edb.get_usercache_db().distinct('user_id')

def get_plottable_df(user_id, loc_filter, start_dt, end_dt):
    tempSection = ad.AttrDict()
    tempSection.user_id = user_id
    tempSection.loc_filter = loc_filter
    if (start_dt is not None and end_dt is not None):
        tempSection.start_ts = time.mktime(start_dt.timetuple()) * 1000
        tempSection.end_ts = time.mktime(end_dt.timetuple()) * 1000
    return get_points_for_section(tempSection)

from_micros = lambda x: pydt.datetime.fromtimestamp(x/1000)

def get_activities_for_section(section):
    query = {"user_id": section.user_id,
             "metadata.filter": section.loc_filter,
             "metadata.key": "background/activity"}

    start_ts = section.start_ts
    end_ts = section.end_ts
    query.update({'$and': [{'metadata.write_ts': {'$gt': start_ts}},
                           {'metadata.write_ts': {'$lt': end_ts}}]})

    full_entries = list(edb.get_usercache_db().find(query))
    merged_entries = [dict(entry["metadata"].items() + entry["data"].items()) for entry in full_entries]
    entries_df = pd.DataFrame(merged_entries)
    entries_df['formatted_time'] = entries_df.write_ts.apply(from_micros)
    entries_df['activity'] = entries_df.agb.apply(to_activity_enum)
    return entries_df

def get_transitions_df(user_id, loc_filter, start_dt, end_dt):
    query = {"user_id": user_id,
             "metadata.filter": loc_filter,
             "metadata.key": "statemachine/transition"}

    if (start_dt is not None and end_dt is not None):
        start_ts = time.mktime(start_dt.timetuple()) * 1000
        end_ts = time.mktime(end_dt.timetuple()) * 1000
        query.update({'$and': [{'metadata.write_ts': {'$gt': start_ts}},
                               {'metadata.write_ts': {'$lt': end_ts}}]})

    full_entries = list(edb.get_usercache_db().find(query))
    merged_entries = [dict(entry["metadata"].items() + entry["data"].items()) for entry in full_entries]
    entries_df = pd.DataFrame(merged_entries)
    entries_df['formatted_time'] = entries_df.write_ts.apply(from_micros)
    return entries_df

def get_points_for_transitions(user_id, transitions_df):
    get_section = lambda transition: ad.AttrDict({'user_id': user_id,
                                    'loc_filter': transition["filter"],
                                    'start_ts': transition["write_ts"] - 10 * 60 * 1000,
                                    'end_ts': transition["write_ts"] + 10})
    get_last_point = lambda transition: get_points_for_section(get_section(transition)).iloc[-1] 
    return transitions_df.apply(get_last_point, axis=1)

def get_points_for_section(section):
    query = {"user_id": section.user_id,
             "metadata.filter": section.loc_filter,
             "metadata.key": "background/location"}

    try:
        query.update({'$and': [{'data.mTime': {'$gt': section.start_ts}},
                               {'data.mTime': {'$lt': section.end_ts}}]})
    except AttributeError:
        logging.debug("Start and end times not defined, no time query")

    print("final query = %s " % query)
    # full_entries = list(edb.get_usercache_db().find(query).sort("data.mTime", pymongo.ASCENDING))
    full_entries = list(edb.get_usercache_db().find(query))
    merged_entries = [dict(entry["metadata"].items() + entry["data"].items()) for entry in full_entries]
    entries_df = pd.DataFrame(merged_entries)
    entries_df['formatted_time'] = entries_df.mTime.apply(from_micros)
    return entries_df

def get_section(section_id):
    section_json = edb.get_section_db().find_one({"id": section_id})
    if section_json is None:
        logging.warning("Did not find match for section %s, returning None" % section_id)
        return None
    return ad.AttrDict(section_json)

def get_potential_split_index(df):
    inter_arrival_times = df.mTime.diff()
    potential_splits = inter_arrival_times[inter_arrival_times > 5000 * 60].index
    potential_splits = potential_splits.insert(0, min(df.index))
    potential_splits = potential_splits.insert(len(potential_splits), max(df.index))
    return potential_splits

def filter_low_accuracy(df, threshold):
    return df[df.mAccuracy > threshold]

def get_mode_query(mode_enum_list):
    mode_value_list = [me.value for me in mode_enum_list]
    ret_val = {'data.mode': {'$in': mode_value_list}}
    logging.debug("in get_mode_query, returning %s" % ret_val)
    return ret_val

