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

import emission.core.get_database as edb
import emission.storage.timeseries.abstract_timeseries as esta

import emission.core.wrapper.entry as ecwe

ts_enum_map = {
    esta.EntryType.DATA_TYPE: edb.get_timeseries_db(),
    esta.EntryType.ANALYSIS_TYPE: edb.get_analysis_timeseries_db()
}

INVALID_QUERY = {'metadata.key': 'invalid'}

class BuiltinTimeSeries(esta.TimeSeries):
    def __init__(self, user_id):
        super(BuiltinTimeSeries, self).__init__(user_id)
        self.key_query = lambda key: {"metadata.key": key}
        self.type_query = lambda entry_type: {"metadata.type": entry_type}
        self.user_query = {"user_id": self.user_id} # UUID is mandatory for this version
        self.timeseries_db = ts_enum_map[esta.EntryType.DATA_TYPE]
        self.analysis_timeseries_db = ts_enum_map[esta.EntryType.ANALYSIS_TYPE]
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
        self.ts_map = {
                "background/location": self.timeseries_db,
                "background/filtered_location": self.timeseries_db,
                "background/motion_activity": self.timeseries_db,
                "background/battery": self.timeseries_db,
                "statemachine/transition": self.timeseries_db,
                "config/sensor_config": self.timeseries_db,
                "config/sync_config": self.timeseries_db,
                "config/consent": self.timeseries_db,
                "stats/server_api_time": self.timeseries_db,
                "stats/server_api_error": self.timeseries_db,
                "stats/pipeline_time": self.timeseries_db,
                "stats/pipeline_error": self.timeseries_db,
                "stats/client_time": self.timeseries_db,
                "stats/client_nav_event": self.timeseries_db,
                "stats/client_error": self.timeseries_db,
                "manual/incident": self.timeseries_db,
                "manual/mode_confirm": self.timeseries_db,
                "manual/purpose_confirm": self.timeseries_db,
                "manual/destination_confirm": self.timeseries_db,
                "segmentation/raw_trip": self.analysis_timeseries_db,
                "segmentation/raw_place": self.analysis_timeseries_db,
                "segmentation/raw_section": self.analysis_timeseries_db,
                "segmentation/raw_stop": self.analysis_timeseries_db,
                "segmentation/raw_untracked": self.analysis_timeseries_db,
                "analysis/smoothing": self.analysis_timeseries_db,
                "analysis/cleaned_trip": self.analysis_timeseries_db,
                "analysis/cleaned_place": self.analysis_timeseries_db,
                "analysis/cleaned_section": self.analysis_timeseries_db,
                "analysis/cleaned_stop": self.analysis_timeseries_db,
                "analysis/cleaned_untracked": self.analysis_timeseries_db,
                "analysis/recreated_location": self.analysis_timeseries_db,
                "metrics/daily_user_count": self.analysis_timeseries_db,
                "metrics/daily_mean_count": self.analysis_timeseries_db,
                "metrics/daily_user_distance": self.analysis_timeseries_db,
                "metrics/daily_mean_distance": self.analysis_timeseries_db,
                "metrics/daily_user_duration": self.analysis_timeseries_db,
                "metrics/daily_mean_duration": self.analysis_timeseries_db,
                "metrics/daily_user_median_speed": self.analysis_timeseries_db,
                "metrics/daily_mean_median_speed": self.analysis_timeseries_db,
                "inference/prediction": self.analysis_timeseries_db,
                "analysis/inferred_section": self.analysis_timeseries_db
            }


    @staticmethod
    def get_uuid_list():
        return edb.get_timeseries_db().distinct("user_id")

    def get_timeseries_db(self, key):
        """
        Return the correct timeseries for the key. Analysis results go into the
        analysis timeseries and raw sensor data stays in the regular timeseries.
        """
        ret_val = self.ts_map[key]
        # logging.debug("Returning %s" % ret_val)
        return ret_val

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
        ret_query = {}
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

    def _get_sort_key(self, time_query = None):
        if time_query is None:
            return "metadata.write_ts"
        elif time_query.timeType.endswith("local_dt"):
            return time_query.timeType.replace("local_dt", "ts")
        else:
            return time_query.timeType

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

    def df_row_to_entry(self, key, row):
        return self.get_entry_from_id(key, row['_id'])

    def get_entry_from_id(self, key, entry_id):
        entry_doc = self.get_timeseries_db(key).find_one({"_id": entry_id})
        if entry_doc is None:
            return None
        else:
            return ecwe.Entry(entry_doc)

    def _split_key_list(self, key_list):
        if key_list is None:
            return (None, None)
        orig_ts_db_keys = [key for key in key_list if 
            self.get_timeseries_db(key) == self.timeseries_db]
        analysis_ts_db_keys = [key for key in key_list if 
            self.get_timeseries_db(key) == self.analysis_timeseries_db]
        return (orig_ts_db_keys, analysis_ts_db_keys)

    def find_entries(self, key_list = None, time_query = None, geo_query = None,
                     extra_query_list=None):
        sort_key = self._get_sort_key(time_query)
        logging.debug("curr_query = %s, sort_key = %s" % 
            (self._get_query(key_list, time_query, geo_query,
                             extra_query_list), sort_key))
        (orig_ts_db_keys, analysis_ts_db_keys) = self._split_key_list(key_list)
        logging.debug("orig_ts_db_keys = %s, analysis_ts_db_keys = %s" % 
            (orig_ts_db_keys, analysis_ts_db_keys))

        orig_ts_db_result = self._get_entries_for_timeseries(self.timeseries_db,
                                                             orig_ts_db_keys,
                                                             time_query,
                                                             geo_query,
                                                             extra_query_list,
                                                             sort_key)

        analysis_ts_db_result = self._get_entries_for_timeseries(self.analysis_timeseries_db,
                                                                 analysis_ts_db_keys,
                                                                 time_query,
                                                                 geo_query,
                                                                 extra_query_list,
                                                                 sort_key)
        logging.debug("orig_ts_db_matches = %s, analysis_ts_db_matches = %s" %
            (orig_ts_db_result.count(), analysis_ts_db_result.count()))
        return itertools.chain(orig_ts_db_result, analysis_ts_db_result)

    def _get_entries_for_timeseries(self, tsdb, key_list, time_query, geo_query,
                                    extra_query_list, sort_key):
        # workaround for https://github.com/e-mission/e-mission-server/issues/271
        # during the migration
        if key_list is None or len(key_list) > 0:
            ts_db_cursor = tsdb.find(
                self._get_query(key_list, time_query, geo_query,
                                extra_query_list))
            if sort_key is None:
                ts_db_result = ts_db_cursor
            else:
                ts_db_result = ts_db_cursor.sort(sort_key, pymongo.ASCENDING)
            # We send the results from the phone in batches of 10,000
            # And we support reading upto 100 times that amount at a time, so over
            # This is more than the number of entries across all metadata types for
            # normal user processing for over a year
            # In [590]: edb.get_timeseries_db().find({"user_id": UUID('08b31565-f990-4d15-a4a7-89b3ba6b1340')}).count()
            # Out[590]: 625272
            #
            # In [593]: edb.get_timeseries_db().find({"user_id": UUID('ea59084e-11d4-4076-9252-3b9a29ce35e0')}).count()
            # Out[593]: 449869
            ts_db_result.limit(25 * 10000)
        else:
            ts_db_result = tsdb.find(INVALID_QUERY)

        logging.debug("finished querying values for %s, count = %d" % (key_list, ts_db_result.count()))
        return ts_db_result

    def get_entry_at_ts(self, key, ts_key, ts):
        import numpy as np

        query_ts = float(ts) if type(ts) == np.int64 or type(ts) == np.float64 else ts
        query = {"user_id": self.user_id, "metadata.key": key, ts_key: query_ts}
        logging.debug("get_entry_at_ts query = %s" % query)
        retValue = self.get_timeseries_db(key).find_one(query)
        logging.debug("get_entry_at_ts result = %s" % retValue)
        return retValue

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
        result_it = self.find_entries([key], time_query, geo_query, extra_query_list)
        return self.to_data_df(key, result_it, map_fn)

    @staticmethod
    def to_data_df(key, entry_it, map_fn = None):
        """
        Converts the specified iterator into a dataframe
        :param key: The key whose entries are in the iterator
        :param it: The iterator to be converted
        :return: A dataframe composed of the entries in the iterator
        """
        if map_fn is None:
            map_fn = BuiltinTimeSeries._to_df_entry
        # Dataframe doesn't like to work off an iterator - it wants everything in memory
        df = pd.DataFrame([map_fn(e) for e in entry_it])
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


    def get_max_value_for_field(self, key, field, time_query=None):
        """
        Currently used to get the max value of the location values so that we can send data
        that actually exists into the usercache. Is that too corner of a use case? Do we want to do
        this in some other way?
        :param key: the metadata key for the entries, used to identify the stream
        :param field: the field in the stream whose max value we want.
        :param time_query: the time range in which to search the stream
        It is assumed that the values for the field are sortable.
        :return: the max value for the field in the stream identified by key. -1 if there are no entries for the key.
        """
        result_it = self.get_timeseries_db(key).find(self._get_query([key], time_query),
                                                 {"_id": False, field: True}).sort(field, pymongo.DESCENDING).limit(1)
        if result_it.count() == 0:
            return -1

        retVal = list(result_it)[0]
        field_parts = field.split(".")
        for part in field_parts:
            retVal = retVal[part]
        return retVal

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

    def insert_data(self, user_id, key, data):
        """
        Inserts an element for this entry when the data is specified, inserts
        it and returns the object ID
        """
        logging.debug("insert_data called")
        entry = ecwe.Entry.create_entry(user_id, key, data)
        return self.insert(entry)

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
        edb.get_timeseries_error_db().insert(entry)

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

