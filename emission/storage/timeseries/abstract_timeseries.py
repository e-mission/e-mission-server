import logging

class TimeSeries(object):
    @staticmethod
    def get_time_series(uuid):
        import emission.storage.timeseries.builtin_timeseries as bits
        return bits.BuiltinTimeSeries(uuid)

    def __init__(self, uuid):
        self.uuid = uuid

    def find_entries(self, key_list = None, time_query = None):
        """
        Find the entries for the specified time query
        """
        pass

    def insert(self, entry):
        pass

    def insert_error(self, entry):
        pass
