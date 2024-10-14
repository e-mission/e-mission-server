import logging

import pandas as pd
import pymongo

import emission.core.get_database as edb
import emission.storage.timeseries.builtin_timeseries as bits
import emission.core.wrapper.entry as ecwe  # Added missing import
import emission.storage.timeseries.aggregate_timeseries as esta

class NonUserTimeSeries(bits.BuiltinTimeSeries):
    def __init__(self):
        super(esta.AggregateTimeSeries, self).__init__(None)
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

    def find_entries(self, key_list=None, time_query=None, geo_query=None,
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
            raise AttributeError(
                f"Saving entry {entry} for {entry['user_id']} in non_user_timeseries is not allowed."
            )
        else:
            logging.debug("entry was fine, no need to fix it")

        # Get the collection and log its full name
        collection = self.get_timeseries_db(entry.metadata.key)
        logging.debug(f"Collection used for insertion: {collection.full_name}")

        logging.debug(f"Inserting entry {entry} into timeseries")
        try:
            result = collection.insert_one(entry)
            logging.debug(f"Inserted entry with ID: {result.inserted_id}")
            return result.inserted_id
        except pymongo.errors.PyMongoError as e:
            logging.error(f"Failed to insert entry: {e}")
            raise


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
