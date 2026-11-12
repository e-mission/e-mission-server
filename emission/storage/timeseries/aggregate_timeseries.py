from builtins import *
import logging
from uuid import UUID

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

    def get_distinct_users(self, key_list= None, time_query=None,
                           geo_query=None, extra_query_list=None):
        query_to_use = self._get_query(key_list, time_query, geo_query,
                                       extra_query_list)
        logging.debug("query_to_use = %s" % query_to_use)
        return self.analysis_timeseries_db.find(query_to_use).distinct('user_id')

