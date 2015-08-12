import logging
import emission.core.get_database as edb
import datetime as pydt
import pandas as pd
import time
import pymongo
import attrdict as ad

def get_uuid_list():
    return edb.get_usercache_db().distinct('user_id')

def get_plottable_df(user_id, loc_filter, start_dt, end_dt):
    tempSection = ad.AttrDict()
    tempSection.user_id = user_id
    tempSection.filter = loc_filter
    if (start_dt is not None and end_dt is not None):
        section.startTs = time.mktime(start_dt.timetuple()) * 1000
        section.endTs = time.mktime(end_dt.timetuple()) * 1000
    return get_points_for_section(section)

def get_points_for_section(section):
    query = {"user_id": section.user_id,
             "metadata.filter": section.loc_filter,
             "metadata.key": "background/location"}

    try:
        query.update({'$and': [{'data.mTime': {'$gt': section.start_ts}},
                               {'data.mTime': {'$lt': section.end_ts}}]})
    except AttributeError:
        logging.debug("Start and end times not defined, no time query")

    print "final query = %s " % query
    full_entries = list(edb.get_usercache_db().find(query).sort("data.mTime", pymongo.ASCENDING))
    merged_entries = [dict(entry["metadata"].items() + entry["data"].items()) for entry in full_entries]
    entries_df = pd.DataFrame(merged_entries)
    from_micros = lambda x: pydt.datetime.fromtimestamp(x/1000)
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

