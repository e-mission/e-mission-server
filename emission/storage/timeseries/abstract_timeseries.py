import logging

class TimeSeries(object):
    @staticmethod
    def get_time_series(uuid):
        import emission.storage.timeseries.builtin_timeseries as bits
        return bits.BuiltinTimeSeries(uuid)

    def __init__(self, uuid):
        self.uuid = uuid

    @staticmethod
    def get_uuid_list():
        import emission.storage.timeseries.builtin_timeseries as bits
        return bits.BuiltinTimeSeries.get_uuid_list()

    def find_entries(self, key_list = None, time_query = None):
        """
        Find the entries for the specified time query
        """
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
