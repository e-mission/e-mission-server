import logging

class TimeSeries(object):
    @staticmethod
    def get_time_series(user_id):
        import emission.storage.timeseries.builtin_timeseries as bits
        return bits.BuiltinTimeSeries(user_id)

    @staticmethod
    def get_aggregate_time_series():
        import emission.storage.timeseries.aggregate_timeseries as bita
        return bita.AggregateTimeSeries()

    def __init__(self, user_id):
        self.user_id = user_id

    @staticmethod
    def get_uuid_list():
        import emission.storage.timeseries.builtin_timeseries as bits
        return bits.BuiltinTimeSeries.get_uuid_list()

    def find_entries(self, key_list = None, time_query = None):
        """
        Find the entries for the specified time query
        """
        pass

    def get_entry_at_ts(self, key, ts_key, ts):
        pass

    def get_data_df(self, key, time_query = None):
        """
        Returns a dataframe of the specified entries. A single key is required,
        since we want to retrieve objects of the same type - the dataframe is
        intended to be a tabular structure and expects each entry to largely
        have the same set of fields.
        """
        pass

    def get_max_value_for_field(self, key, field):
        """
        Currently used to get the max value of the location values so that we can send data
        that actually exists into the usercache. Is that too corner of a use case? Do we want to do
        this in some other way?
        :param key: the metadata key for the entries, used to identify the stream
        :param field: the field in the stream whose max value we want.
        It is assumed that the values for the field are sortable.
        :return: the max value for the field in the stream identified by key.  -1 if there are no entries for the key.
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
