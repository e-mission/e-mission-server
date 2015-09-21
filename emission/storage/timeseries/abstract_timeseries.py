import logging

class TimeSeries(object):
    @staticmethod
    def get_time_series(user_id):
        import emission.storage.timeseries.builtin_timeseries as bits
        return bits.BuiltinTimeSeries(user_id)

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

    def insert(self, entry):
        pass

    def insert_error(self, entry):
        pass
