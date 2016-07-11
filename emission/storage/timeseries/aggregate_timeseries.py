import logging
import pandas as pd
import pymongo

import emission.core.get_database as edb
import emission.storage.timeseries.builtin_timeseries as bits

class AggregateTimeSeries(bits.BuiltinTimeSeries):
    def __init__(self):
        super(AggregateTimeSeries, self).__init__(None)
        self.user_query = {}

    def _get_sort_key(self, time_query = None):
        return None
