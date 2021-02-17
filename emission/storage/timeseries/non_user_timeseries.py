import logging

import pandas as pd
import pymongo

import emission.core.get_database as edb
import emission.storage.timeseries.builtin_timeseries as bits

class NonUserTimeSeries(bits.BuiltinTimeSeries):
    def __init__(self):
        super(AggregateTimeSeries, self).__init__(None)
        self.user_query = {}
        self.timeseries_db = edb.get_non_user_timeseries_db()

    @staticmethod
    def get_uuid_list():
        return []

    def get_timeseries_db(self, key):
        return self.timeseries_db

    # _get_query: not overridden
    # _get_sort_query: not overridden
    # _to_df_entry: not overridden
    # df_row_to_entry: not overridden
    # get_entry_from_id: not overridden

    def find_entries(self, key_list = None, time_query = None, geo_query = None,
                     extra_query_list=None):
        sort_key = self._get_sort_key(time_query)
        logging.debug("curr_query = %s, sort_key = %s" % 
            (self._get_query(key_list, time_query, geo_query,
                             extra_query_list), sort_key))
        ts_db_result = self._get_entries_for_timeseries(self.timeseries_db,
                                                             key_list,
                                                             time_query,
                                                             geo_query,
                                                             extra_query_list,
                                                             sort_key)
        return ts_db_result

    # _get_entries_for_timeseries is unchanged
    # get_entry_at_ts is unchanged
    # get_data_df is unchanged
    # to_data_df is unchanged
    # get_first_value_for_field is unchanged
    # bulk_insert is unchanged

    def insert(self, entry):
        """
        Inserts the specified entry and returns the object ID 
        """
        logging.debug("insert called")
        if type(entry) == dict:
            entry = ecwe.Entry(entry)
        if entry["user_id"] is not None:
            raise AttributeError("Saving entry %s for %s in non_user_timeseries" % 
                (entry, entry["user_id"]))
        else:
            logging.debug("entry was fine, no need to fix it")

        logging.debug("Inserting entry %s into timeseries" % entry)
        return self.get_timeseries_db(entry.metadata.key).insert(entry)

    # insert_data is unchanged
    def insert_error(self, entry):
        """
        """
        raise AttributeError("non_user_timeseries has no error database")
    
    @staticmethod
    def update(entry):
        """
        Save the specified entry. In general, our entries are read-only, so
        this should only be called under very rare conditions. Once we identify
        what these conditions are, we should consider replacing them with
        versioned objects
        """
        raise AttributeError("non_user_timeseries does not support updates")

    @staticmethod
    def update_data(user_id, key, obj_id, data):
        """
        Save the specified entry. In general, our entries are read-only, so
        this should only be called under very rare conditions. Once we identify
        what these conditions are, we should consider replacing them with
        versioned objects
        """
        raise AttributeError("non_user_timeseries does not support updates")

