from __future__ import unicode_literals
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import
from future import standard_library
standard_library.install_aliases()
from builtins import *
from builtins import object
import logging
import enum as enum

class EntryType(enum.Enum):
    DATA_TYPE = 0
    ANALYSIS_TYPE = 1

class TimeSeries(object):
    @staticmethod
    def get_time_series(user_id):
        import emission.storage.timeseries.builtin_timeseries as bits
        return bits.BuiltinTimeSeries(user_id)

    @staticmethod
    def get_aggregate_time_series():
        import emission.storage.timeseries.aggregate_timeseries as bita
        return bita.AggregateTimeSeries()

    @staticmethod
    def get_non_user_time_series():
        import emission.storage.timeseries.non_user_timeseries as nuts
        return nuts.NonUserTimeSeries()

    def __init__(self, user_id):
        self.user_id = user_id

    @staticmethod
    def get_uuid_list():
        import emission.storage.timeseries.builtin_timeseries as bits
        return bits.BuiltinTimeSeries.get_uuid_list()

    def find_entries(self, key_list=None, time_query=None, geo_query=None,
                     extra_query_list=None):
        """
        Find the entries for the specified time query
        :param geo_query:
        :param extra_query_list:
        """
        pass

    def get_entry_at_ts(self, key, ts_key, ts):
        pass

    def get_entry_from_id(self, key, entry_id):
        pass

    def get_data_df(self, key, time_query = None, geo_query=None, extra_query_list=None):
        """
        Returns a dataframe of the specified entries. A single key is required,
        since we want to retrieve objects of the same type - the dataframe is
        intended to be a tabular structure and expects each entry to largely
        have the same set of fields.
        """
        pass

    def get_first_value_for_field(self, key, sort_order, field):
        """
        Currently used to get the max value of the location values so that we can send data
        that actually exists into the usercache. Is that too corner of a use case? Do we want to do
        this in some other way?
        :param key: the metadata key for the entries, used to identify the stream
        :param field: the field in the stream whose max value we want.
        :param time_query: the time range in which to search the stream
        :param sort_order: pymongo.ASCENDING or pymongon.DESCENDING
        It is assumed that the values for the field are sortable.
        :return: the max value for the field in the stream identified by key.  -1 if there are no entries for the key.
        """
        pass

    def bulk_insert(self, entries, entry_type=None):
        """
        Inserts the specified interable into the specified database, using
        the bulk insert operation of the underlying timeseries.
        :param entries: The iterable of entry objects to be inserted
        :param entry_type: The database (data or analysis) to be inserted into. If
        not specified, we will automatically figure out the database based on the
        keys. However, this will make the operation slower, since we need to group
        entries by their key, and group_by requires a sorted array.

        However, specifying a database requires all entries to be stored into
        the same database, but inferring the database allows greater flexibility.
        :return:
        """
        pass

    def insert(self, entry):
        pass

    def insert_data(self, user_id, key, data):
        pass

    def insert_error(self, entry):
        pass

    @staticmethod
    def update(entry):
        pass

    @staticmethod
    def update_data(user_id, key, obj_id, data):
        pass

    def invalidate_raw_entry(self, obj_id):
        pass
