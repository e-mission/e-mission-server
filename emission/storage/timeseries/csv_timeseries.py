from __future__ import unicode_literals
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import
from future import standard_library
standard_library.install_aliases()
from builtins import *
import logging
import pandas as pd
import pymongo
import itertools
from typing import Tuple
import arrow


import emission.core.get_database as edb
import emission.storage.timeseries.abstract_timeseries as esta

import emission.core.wrapper.entry as ecwe
import emission.core.wrapper.metadata as ecwm
import emission.core.wrapper.localdate as ecwld

def _get_enum_map():
    return {
        esta.EntryType.DATA_TYPE: edb.get_timeseries_db(),
        esta.EntryType.ANALYSIS_TYPE: edb.get_analysis_timeseries_db()
    }

ts_enum_map = _get_enum_map()

class CsvTimeSeries(esta.TimeSeries):
    def __init__(self, user_id):
        super(CsvTimeSeries, self).__init__(user_id)
        # self.key_query = lambda key: {"metadata.key": key}
        # self.type_query = lambda entry_type: {"metadata.type": entry_type}
        self.user_query = {"user_id": self.user_id} # UUID is mandatory for this version

        self.analysis_timeseries_db = self._process_csv('./data/analysis_confirmed_trip.csv')

        #maintain only user dataframe in this version
        # self.analysis_timeseries_db = self.analysis_timeseries_db[self.analysis_timeseries_db['perno'] == self.user_id]
        # self.analysis_timeseries_db = ts_enum_map[esta.EntryType.ANALYSIS_TYPE]
        # Design question: Should the stats be a separate database, or should it be part
        # of the timeseries database? Technically, it should be part of the timeseries
        # database. However, I am concerned about the performance of the database
        # with even more entries - it already takes 10 seconds to query for a document
        # and I am not sure that adding a ton more data is going to make that better
        # it is going to be easier to copy entries into the same database instead of
        # splitting out, and we already support multiple indices, so I am tempted to put
        # it separately. On the other hand, then the load/store from timeseries won't work
        # if it is a separate database. Let's do the right thing and change the storage/
        # shift to a different timeseries if we need to

        # TODO: Remake this map to contain adresses to the csvs
        self.ts_map = {
                # "background/location": self.timeseries_db,
                # "background/filtered_location": self.timeseries_db,
                # "background/motion_activity": self.timeseries_db,
                # "background/battery": self.timeseries_db,
                # "background/bluetooth_ble": self.timeseries_db,
                # "statemachine/transition": self.timeseries_db,
                # "config/sensor_config": self.timeseries_db,
                # "config/sync_config": self.timeseries_db,
                # "config/consent": self.timeseries_db,
                # "config/app_ui_config": self.timeseries_db,
                # "stats/server_api_time": self.timeseries_db,
                # "stats/server_api_error": self.timeseries_db,
                # "stats/pipeline_time": self.timeseries_db,
                # "stats/dashboard_time": self.timeseries_db,
                # "stats/dashboard_error": self.timeseries_db,
                # "stats/pipeline_error": self.timeseries_db,
                # "stats/client_time": self.timeseries_db,
                # "stats/client_nav_event": self.timeseries_db,
                # "stats/client_error": self.timeseries_db,
                # "manual/incident": self.timeseries_db,
                # "manual/mode_confirm": self.timeseries_db,
                # "manual/purpose_confirm": self.timeseries_db,
                # "manual/replaced_mode": self.timeseries_db,
                # "manual/trip_user_input": self.timeseries_db,
                # "manual/place_user_input": self.timeseries_db,
                # "manual/trip_addition_input": self.timeseries_db,
                # "manual/place_addition_input": self.timeseries_db,
                # "manual/demographic_survey": self.timeseries_db,
                # "segmentation/raw_trip": self.analysis_timeseries_db,
                # "segmentation/raw_place": self.analysis_timeseries_db,
                # "segmentation/raw_section": self.analysis_timeseries_db,
                # "segmentation/raw_stop": self.analysis_timeseries_db,
                # "segmentation/raw_untracked": self.analysis_timeseries_db,
                # "analysis/smoothing": self.analysis_timeseries_db,
                # "analysis/cleaned_trip": self.analysis_timeseries_db,
                "analysis/cleaned_place": self.analysis_timeseries_db,
                "analysis/cleaned_section": self.analysis_timeseries_db,
                "analysis/cleaned_stop": self.analysis_timeseries_db,
                # "analysis/cleaned_untracked": self.analysis_timeseries_db,
                # "analysis/recreated_location": self.analysis_timeseries_db,
                # "metrics/daily_user_count": self.analysis_timeseries_db,
                # "metrics/daily_mean_count": self.analysis_timeseries_db,
                # "metrics/daily_user_distance": self.analysis_timeseries_db,
                # "metrics/daily_mean_distance": self.analysis_timeseries_db,
                # "metrics/daily_user_duration": self.analysis_timeseries_db,
                # "metrics/daily_mean_duration": self.analysis_timeseries_db,
                # "metrics/daily_user_median_speed": self.analysis_timeseries_db,
                # "metrics/daily_mean_median_speed": self.analysis_timeseries_db,
                # "inference/prediction": self.analysis_timeseries_db,
                # "inference/labels": self.analysis_timeseries_db,
                # "inference/trip_model": self.analysis_timeseries_db,
                # "analysis/inferred_section": self.analysis_timeseries_db,
                # "analysis/inferred_labels": self.analysis_timeseries_db,
                # "analysis/inferred_trip": self.analysis_timeseries_db,
                # "analysis/expected_trip": self.analysis_timeseries_db,
                "analysis/confirmed_trip": self.analysis_timeseries_db,
                # "analysis/confirmed_section": self.analysis_timeseries_db,
                # "analysis/confirmed_place": self.analysis_timeseries_db,
                # "analysis/confirmed_untracked": self.analysis_timeseries_db,
                # "analysis/composite_trip": self.analysis_timeseries_db
            }
    
    
    def _process_csv(self, file):
        """
        WIP.
        Construct the necesarry features of the dataframe so it can function like the time series quickly.
        Add optimizations wherever possible so the overall size of the datafram is minimized
        """
        df = pd.read_csv(file)
        df = df[df['perno'] == self.user_id]
        
        def make_metadata_write_ts(row):    
            # Using datetime object
            local=arrow.now(row['metadata_write_local_dt_timezone']).tzinfo
            time_stamp = arrow.Arrow(row['metadata_write_local_dt_year'], row['metadata_write_local_dt_month'], row['metadata_write_local_dt_day'], tzinfo=local)
            return time_stamp.int_timestamp, time_stamp.isoformat()
        df['metadata_write_fmt_time'] = df.apply(lambda x: make_metadata_write_ts(x)[1], axis=1)
        df['metadata_write_ts'] = df.apply(lambda x: make_metadata_write_ts(x)[0], axis=1)

        def make_data_start_ts(row):    
            # Using datetime object
            local=arrow.now(row['data_start_local_dt_timezone']).tzinfo
            time_stamp = arrow.Arrow(row['data_start_local_dt_year'], row['data_start_local_dt_month'], row['data_start_local_dt_day'], tzinfo=local)
            return time_stamp.int_timestamp, time_stamp.isoformat()
        df['data_start_fmt_time'] = df.apply(lambda x: make_data_start_ts(x)[1], axis=1)
        df['data_start_ts'] = df.apply(lambda x: make_data_start_ts(x)[0], axis=1)

        def make_data_end_ts(row):    
            # Using datetime object
            local=arrow.now(row['data_end_local_dt_timezone']).tzinfo
            time_stamp = arrow.Arrow(row['data_end_local_dt_year'], row['data_end_local_dt_month'], row['data_end_local_dt_day'], tzinfo=local)
            return time_stamp.int_timestamp, time_stamp.isoformat()
        df['data_end_fmt_time'] = df.apply(lambda x: make_data_end_ts(x)[1], axis=1)
        df['data_endt_ts'] = df.apply(lambda x: make_data_end_ts(x)[0], axis=1)


        return df

    #TODO
    @staticmethod
    def get_uuid_list():
        return edb.get_uuid_db().distinct("uuid")
    
    #TODO
    def get_timeseries_db(self, key):
        """
        Return the correct timeseries for the key. Analysis results go into the
        analysis timeseries and raw sensor data stays in the regular timeseries.
        """
        ret_val = self.ts_map[key]
        # logging.debug("Returning %s" % ret_val)
        return ret_val

    #TODO
    def _get_query(self, key_list = None, time_query = None, geo_query = None,
                   extra_query_list = []):
        """
        The extra query list cannot contain a top level field from
        one of the existing queries, otherwise it will be overwritten
        by the extra query
        :param key_list: list of metadata keys to query
        :param time_query: time range or time components (filter)
        :param geo_query: $geoWithin query
        :param extra_query_list: additional queries for mode, etc
        :return:
        """
        ret_query = {"invalid": {"$exists": False}}
        ret_query.update(self.user_query)
        if key_list is not None and len(key_list) > 0:
            key_query_list = []
            for key in key_list:
                key_query_list.append(self.key_query(key))
            ret_query.update({"$or": key_query_list})
        if time_query is not None:
            ret_query.update(time_query.get_query())
        if geo_query is not None:
            ret_query.update(geo_query.get_query())
        if extra_query_list is not None:
            for extra_query in extra_query_list:
                eq_keys = set(extra_query.keys())
                curr_keys = set(ret_query.keys())
                overlap_keys = eq_keys.intersection(curr_keys)
                if len(overlap_keys) != 0:
                    logging.info("eq_keys = %s, curr_keys = %s, overlap_keys = %s" %
                                 (eq_keys, curr_keys, overlap_keys))
                    raise AttributeError("extra query would overwrite keys %s" %
                                         list(overlap_keys))
                else:
                    ret_query.update(extra_query)
        return ret_query

    #TODO
    def _time_query_to_col(self, time_query = None):
        if time_query is None:
            return "metadata_write_ts"
        # elif time_query.timeType.endswith("local_dt"):
        #     return time_query.timeType.replace("local_dt", "ts")
        else:
            return time_query.timeType.replace(".", "_")

    #TODO
    @staticmethod
    def _to_df_entry(entry_dict):
        entry = ecwe.Entry(entry_dict)
        ret_val = entry.data
        for ld_key in ret_val.local_dates:
            if ld_key in ret_val:
                for field_key in ret_val[ld_key]:
                    expanded_key = "%s_%s" % (ld_key,field_key)
                    ret_val[expanded_key] = ret_val[ld_key][field_key]
                del ret_val[ld_key]
        ret_val["_id"] = entry["_id"]
        ret_val['user_id'] = entry['user_id']
        ret_val["metadata_write_ts"] = entry["metadata"]["write_ts"]
        # logging.debug("ret_val = %s " % ret_val)
        return ret_val

    #TODO
    def df_row_to_entry(self, key, row):
        return self.get_entry_from_id(key, row['_id'])

    #TODO
    def get_entry_from_id(self, key, entry_id):
        entry_doc = self.get_timeseries_db(key).find_one({"_id": entry_id})
        if entry_doc is None:
            return None
        else:
            return ecwe.Entry(entry_doc)

    #TODO: No longer works
    def _split_key_list(self, key_list):
        if key_list is None:
            return (None, None)
        orig_ts_db_keys = [key for key in key_list if 
            self.get_timeseries_db(key) == self.timeseries_db]
        analysis_ts_db_keys = [key for key in key_list if 
            self.get_timeseries_db(key) == self.analysis_timeseries_db]
        return (orig_ts_db_keys, analysis_ts_db_keys)
    
    def _row_to_entry(self, key, row):
        """
        Converts the specified row into an entry
        :param key: The key for the chosen csv
        :param row: The row to be converted
        :return: An entry for the row
        """
        entry_data = row.to_dict()

        m = ecwm.Metadata()
        m.key = key
        m.platform = entry_data['metadata_platform']
        m.write_ts = entry_data['metadata_write_ts']
        m.time_zone = entry_data['metadata_write_local_dt_timezone']
        m.write_local_dt = ecwld.LocalDate.get_local_date(m.write_ts, m.time_zone)
        m.write_fmt_time = arrow.get(m.write_ts).to(m.time_zone).isoformat()

        result_entry = ecwe.Entry()
        result_entry['_id'] = entry_data['_id']
        result_entry.user_id = entry_data['perno']
        result_entry.metadata = m
        result_entry.data = entry_data #TODO: Not sure if metadata keys should be filtered
        return result_entry

    #TODO
    def find_entries(self, key_list = None, time_query = None, geo_query = None,
                     extra_query_list=None):
        
        #TODO: Load in the data from the chosen csv files, 

        time_col = self._time_query_to_col(time_query)
        

        df = self.analysis_timeseries_db[(self.analysis_timeseries_db[time_col] >= time_query.startTs) 
                                         & (self.analysis_timeseries_db[time_col] <= time_query.endTs)].sort_values(by=[time_col])
        #TODO: Geo query
        
        entry_list = []

        for _, row in df.iterrows():
            entry_list.append(self._row_to_entry(key_list[0], row)) #TODO: Only one key is supported for now

        return entry_list

        """
        https://github.com/e-mission/em-public-dashboard/blob/31b0e96476c1b8400c7761fc455925403b7b10d0/viz_scripts/scaffolding.py#L64
scaffolding.py
e-mission/em-public-dashboard
 
in the public dashboard, we also read the entries and use get_data_df, we don't actually go from the dataframe to entries
 
but we do munge the trip entries to be in the format that the footprint calculator expects, so that could be an example of how you might want to create trips
 
        """


    #TODO
    def get_entry_at_ts(self, key, ts_key, ts):
        import numpy as np
        #TODO: Load in the data from the chosen csv files, 

        query_ts = float(ts) if type(ts) == np.int64 or type(ts) == np.float64 else ts
        time_col = self._time_query_to_col(ts_key)
        
        return self._row_to_entry(key, self.analysis_timeseries_db[(self.analysis_timeseries_db[time_col] == query_ts)].iloc[0]) 

    #TODO
    def get_data_df(self, key, time_query = None, geo_query = None,
                    extra_query_list=None,
                    map_fn = None):
        """
        Retuns a dataframe for the specified query.
        :param key: the metadata key we are querying for. Only supports one key
        since a dataframe pretty much implies that all entries have the same structure
        :param time_query: the query for the time
        :param geo_query: the query for a geographical area
        :param extra_query_list: any additional filters (used to filter out the
        test phones, for example)
        :param map_fn: the function that maps the entry to a suitable dict for dataframe conversion
        entry -> dict
        :return:
        """
        time_col = self._time_query_to_col(time_query)
        
        #TODO: Load in the data from the chosen csv files,
        df = self.analysis_timeseries_db[(self.analysis_timeseries_db[time_col] >= time_query.startTs) 
                                         & (self.analysis_timeseries_db[time_col] <= time_query.endTs)].sort_values(by=[time_col])
        #TODO: Geo query
        return df

    #TODO
    @staticmethod
    def to_data_df(key, entries, map_fn = None):
        """
        Converts the specified iterator into a dataframe
        :param key: The key whose entries are in the iterator
        :param it: The iterator to be converted
        :return: A dataframe composed of the entries in the iterator
        """
        if map_fn is None:
            map_fn = CsvTimeSeries._to_df_entry
        # Dataframe doesn't like to work off an iterator - it wants everything in memory
        df = pd.DataFrame([map_fn(e) for e in entries])
        logging.debug("Found %s results" % len(df))
        if len(df) > 0:
            dedup_check_list = [item for item in ecwe.Entry.get_dedup_list(key)
                                if item in df.columns] + ["metadata_write_ts"]
            numeric_check_list = [col for col in dedup_check_list if df[col].dtype != 'object']
            deduped_df = df.drop_duplicates(subset=numeric_check_list)
            logging.debug("After de-duping, converted %s points to %s " %
                          (len(df), len(deduped_df)))
        else:
            deduped_df = df
        return deduped_df.reset_index(drop=True)


    #TODO
    def get_first_entry(self, key, field, sort_order, time_query=None):
        """gets the first entry with the provided key when sorted by some field

        :param key: the metadata key for the entries, used to identify the stream
        :param field: the field in the stream whose max value we want.
        :param sort_order: pymongo.ASCENDING or pymongon.DESCENDING
        :param time_query: the time range in which to search the stream
        :return: a database row, or None if no match is found
        """
        #TODO: Load in the data from the chosen csv files, 

        time_col = self._time_query_to_col(time_query)
        field = field.replace(".", "_")

        df = self.analysis_timeseries_db[(self.analysis_timeseries_db[time_col] >= time_query.startTs) 
                                         & (self.analysis_timeseries_db[time_col] <= time_query.endTs)]
        df.sort_values(by=[field, '_id'], ascending=[sort_order == pymongo.ASCENDING, True])
        return self._row_to_entry(key, df.iloc[0]) if len(df) > 0 else None
    

    #TODO
    def get_first_value_for_field(self, key, field, sort_order, time_query=None):
        """
        Currently used to get the max value of the location values so that we can send data
        that actually exists into the usercache. Is that too corner of a use case? Do we want to do
        this in some other way?
        :param key: the metadata key for the entries, used to identify the stream
        :param field: the field in the stream whose max value we want.
        :param time_query: the time range in which to search the stream
        :param sort_order: pymongo.ASCENDING or pymongon.DESCENDING
        It is assumed that the values for the field are sortable.
        :return: the max value for the field in the stream identified by key. -1 if there are no entries for the key.
        """

        #TODO: Load in the data from the chosen csv files, 
        time_col = self._time_query_to_col(time_query)
        field = field.replace(".", "_")

        df = self.analysis_timeseries_db[(self.analysis_timeseries_db[time_col] >= time_query.startTs) 
                                         & (self.analysis_timeseries_db[time_col] <= time_query.endTs)]
        df.sort_values(by=[field, '_id'], ascending=[sort_order == pymongo.ASCENDING, True])
        return df.iloc[0][field] if len(df) > 0 else -1

    #TODO
    def bulk_insert(self, entries, data_type = None):
        if data_type is None:
            keyfunc = lambda e: e.metadata.key
            sorted_data = sorted(entries, key=keyfunc)
            for k, g in itertools.groupby(sorted_data, keyfunc):
                try:
                    glist = list(g)
                    logging.debug("Inserting %s entries for key %s" % (len(glist), k))
                    self.get_timeseries_db(k).insert_many(glist, ordered=True)
                except pymongo.errors.BulkWriteError as e:
                    logging.info("Got errors %s while saving %d entries for key %s" % 
                        (e.details['writeErrors'], len(glist), k))
        else:
            multi_result = None
            try:
                multi_result = ts_enum_map[data_type].insert_many(entries, ordered=False)
                logging.debug("Returning multi_result.inserted_ids = %s... of length %d" % 
                    (multi_result.inserted_ids[:10], len(multi_result.inserted_ids)))
                return multi_result
            except pymongo.errors.BulkWriteError as e:
                logging.info("Got errors %s while saving %d entries" % 
                    (e.details['writeErrors'], len(entries)))

    #TODO
    def insert(self, entry):
        """
        Inserts the specified entry and returns the object ID 
        """
        logging.debug("insert called with entry of type %s" % type(entry))
        if type(entry) == dict:
            entry = ecwe.Entry(entry)
        if "user_id" not in entry or entry["user_id"] is None:
            entry["user_id"] = self.user_id
        if self.user_id is not None and entry["user_id"] != self.user_id:
            raise AttributeError("Saving entry %s for %s in timeseries for %s" % 
		(entry, entry["user_id"], self.user_id))
        else:
            logging.debug("entry was fine, no need to fix it")

        logging.debug("Inserting entry %s into timeseries" % entry)
        ins_result = self.get_timeseries_db(entry.metadata.key).insert_one(entry)
        return ins_result.inserted_id

    #TODO
    def insert_data(self, user_id, key, data):
        """
        Inserts an element for this entry when the data is specified, inserts
        it and returns the object ID
        """
        logging.debug("insert_data called")
        entry = ecwe.Entry.create_entry(user_id, key, data)
        return self.insert(entry)

    #TODO
    def insert_error(self, entry):
        """
        """
        logging.debug("insert_error called")
        if "user_id" not in entry:
            entry["user_id"] = self.user_id
        elif entry["user_id"] != self.user_id:
            raise AttributeError("Saving entry for %s in timeseries for %s" % (entry["user_id"], self.user_id))
        else:
            logging.debug("entry was fine, no need to fix it")

        logging.debug("Inserting entry %s into error timeseries" % entry)
        edb.get_timeseries_error_db().insert_one(entry)

    #TODO
    @staticmethod
    def update(entry):
        """
        Save the specified entry. In general, our entries are read-only, so
        this should only be called under very rare conditions. Once we identify
        what these conditions are, we should consider replacing them with
        versioned objects
        """
        logging.debug("update called")
        ts = esta.TimeSeries.get_time_series(entry.user_id)
        logging.debug("Saving entry %s into timeseries" % entry)
        edb.save(ts.get_timeseries_db(entry.metadata.key), entry)

    #TODO
    @staticmethod
    def update_data(user_id, key, obj_id, data):
        """
        Save the specified entry. In general, our entries are read-only, so
        this should only be called under very rare conditions. Once we identify
        what these conditions are, we should consider replacing them with
        versioned objects
        """
        logging.debug("update_data called")
        ts = esta.TimeSeries.get_time_series(user_id)
        new_entry = ecwe.Entry.create_entry(user_id, key, data)
        # Make sure that we update the existing entry instead of creating a new one
        new_entry['_id'] = obj_id
        logging.debug("updating entry %s into timeseries" % new_entry)
        edb.save(ts.get_timeseries_db(key), new_entry)

    #TODO
    def invalidate_raw_entry(self, obj_id):
        self.timeseries_db.update_one({"_id": obj_id, "user_id": self.user_id}, {"$set": {"invalid": True}})


    def find_entries_count(self, key_list = None, time_query = None, geo_query = None, extra_query_list = None):
        """
        Returns the total number of documents for the given key_list referring to each of the two timeseries db.

        Input: Key list with keys from both timeseries DBs = [key1, key2, key3, key4, ...]
                Suppose (key1, key2) are orig_tsdb keys and (key3, key4) are analysis_tsdb keys
        Output: total_count = orig_tsdb_count + analysis_tsdb_count
                            
                Orig_tsdb_count and Analysis_tsdb_count are lists containing counts of matching documents 
                for each key considered separately for the specific timeseries DB.

        :param key_list: list of metadata keys we are querying for.
        :param time_query: the time range in which to search the stream
        :param geo_query: the query for a geographical area
        :param extra_query_list: any additional queries to filter out data

        For key_list = None or empty, total count of all documents are returned considering the matching entries from entire dataset.
        """

        if key_list == []:
            key_list = None
        
        # Segregate orig_tsdb and analysis_tsdb keys so as to fetch counts on each dataset
        (orig_tsdb_keys, analysis_tsdb_keys) = self._split_key_list(key_list)

        orig_tsdb_count = len(self.find_entries(orig_tsdb_keys, time_query, geo_query, extra_query_list))
        analysis_tsdb_count= len(self.find_entries(analysis_tsdb_keys, time_query, geo_query, extra_query_list))

        total_matching_count = [orig_tsdb_count, analysis_tsdb_count]
        return total_matching_count

