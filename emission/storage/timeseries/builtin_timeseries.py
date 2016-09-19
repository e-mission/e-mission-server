import logging
import pandas as pd
import pymongo
import itertools

import emission.core.get_database as edb
import emission.storage.timeseries.abstract_timeseries as esta

import emission.core.wrapper.entry as ecwe

class BuiltinTimeSeries(esta.TimeSeries):
    def __init__(self, user_id):
        super(BuiltinTimeSeries, self).__init__(user_id)
        self.key_query = lambda(key): {"metadata.key": key}
        self.type_query = lambda(entry_type): {"metadata.type": entry_type}
        self.user_query = {"user_id": self.user_id} # UUID is mandatory for this version
        self.timeseries_db = edb.get_timeseries_db()
        self.analysis_timeseries_db = edb.get_analysis_timeseries_db()
        self.ts_map = {
                "background/location": self.timeseries_db,
                "background/filtered_location": self.timeseries_db,
                "background/motion_activity": self.timeseries_db,
                "background/battery": self.timeseries_db,
                "statemachine/transition": self.timeseries_db,
                "config/sensor_config": self.timeseries_db,
                "config/sync_config": self.timeseries_db,
                "segmentation/raw_trip": self.analysis_timeseries_db,
                "segmentation/raw_place": self.analysis_timeseries_db,
                "segmentation/raw_section": self.analysis_timeseries_db,
                "segmentation/raw_stop": self.analysis_timeseries_db,
                "analysis/smoothing": self.analysis_timeseries_db,
                "analysis/cleaned_trip": self.analysis_timeseries_db,
                "analysis/cleaned_place": self.analysis_timeseries_db,
                "analysis/cleaned_section": self.analysis_timeseries_db,
                "analysis/cleaned_stop": self.analysis_timeseries_db,
                "analysis/recreated_location": self.analysis_timeseries_db,
                "eval/public_device": self.analysis_timeseries_db
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
        ret_query = self.user_query
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
                ret_query.update(extra_query)
        return ret_query

    def _get_sort_key(self, time_query = None):
        if time_query is None:
            return "metadata.write_ts"
        else:
            return time_query.timeType

    @staticmethod
    def _to_df_entry(entry):
        ret_val = entry["data"]
        ret_val["_id"] = entry["_id"]
        ret_val["metadata_write_ts"] = entry["metadata"]["write_ts"]
        # logging.debug("ret_val = %s " % ret_val)
        return ret_val

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
	# workaround for https://github.com/e-mission/e-mission-server/issues/271
        # during the migration
        if orig_ts_db_keys is None or len(orig_ts_db_keys) > 0:
          orig_ts_db_result = self.timeseries_db.find(
            self._get_query(orig_ts_db_keys, time_query, geo_query)).sort(
            sort_key, pymongo.ASCENDING)
        else:
          orig_ts_db_result = [].__iter__()

        analysis_ts_db_cursor = self.analysis_timeseries_db.find(
            self._get_query(analysis_ts_db_keys, time_query, geo_query))
        if sort_key is None:
            analysis_ts_db_result = analysis_ts_db_cursor
        else:
	    analysis_ts_db_result = analysis_ts_db_cursor.sort(sort_key, pymongo.ASCENDING)
        return itertools.chain(orig_ts_db_result, analysis_ts_db_result)

    def get_entry_at_ts(self, key, ts_key, ts):
        return self.get_timeseries_db(key).find_one({"user_id": self.user_id,
                                                 "metadata.key": key,
                                                 ts_key: ts})

    def get_data_df(self, key, time_query = None, geo_query = None,
                    extra_query_list=None):
        sort_key = self._get_sort_key(time_query)
        logging.debug("curr_query = %s, sort_key = %s" %
                      (self._get_query([key], time_query, geo_query, extra_query_list),
                       sort_key))
        result_it = self.get_timeseries_db(key).find(
            self._get_query([key], time_query, geo_query, extra_query_list),
            {"data": True, "metadata.write_ts": True}).sort(sort_key, pymongo.ASCENDING)
        logging.debug("Found %s results" % result_it.count())
        # Dataframe doesn't like to work off an iterator - it wants everything in memory
        return pd.DataFrame([BuiltinTimeSeries._to_df_entry(e) for e in list(result_it)])

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

    def insert(self, entry):
        """
        Inserts the specified entry and returns the object ID 
        """
        logging.debug("insert called")
        if type(entry) == dict:
            entry = ecwe.Entry(entry)
        if "user_id" not in entry or entry["user_id"] is None:
            entry["user_id"] = self.user_id
        if entry["user_id"] != self.user_id:
            raise AttributeError("Saving entry %s for %s in timeseries for %s" % 
		(entry, entry["user_id"], self.user_id))
        else:
            logging.debug("entry was fine, no need to fix it")

        logging.debug("Inserting entry %s into timeseries" % entry)
        return self.get_timeseries_db(entry.metadata.key).insert(entry)

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
        ts.get_timeseries_db(entry.metadata.key).save(entry)

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
        ts.get_timeseries_db(key).save(new_entry)

