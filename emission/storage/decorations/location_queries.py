import logging
import emission.core.get_database as edb
import datetime as pydt
import pandas as pd
import time
import pymongo

def get_uuid_list():
    return edb.get_usercache_db().distinct('user_id')

def get_plottable_df(user_id, loc_filter, start_dt, end_dt):
    query = {"user_id": user_id,
             "metadata.filter": loc_filter,
             "metadata.key": "background/location"}

    if (start_dt is not None and end_dt is not None):
        start_ts = time.mktime(start_dt.timetuple())
        end_ts = time.mktime(end_dt.timetuple())
        query.update({'$and': [{'data.mTime': {'$gt': start_ts * 1000}},
                               {'data.mTime': {'$lt': end_ts * 1000}}]})

    print "final query = %s " % query
    full_entries = list(edb.get_usercache_db().find(query).sort("data.mTime", pymongo.ASCENDING))
    merged_entries = [dict(entry["metadata"].items() + entry["data"].items()) for entry in full_entries]
    entries_df = pd.DataFrame(merged_entries)
    from_micros = lambda x: pydt.datetime.fromtimestamp(x/1000)
    entries_df['formatted_time'] = entries_df.mTime.apply(from_micros)
    return entries_df

def get_potential_split_index(df):
    inter_arrival_times = df.mTime.diff()
    potential_splits = inter_arrival_times[inter_arrival_times > 5000 * 60].index
    potential_splits = potential_splits.insert(0,0)
    return potential_splits

