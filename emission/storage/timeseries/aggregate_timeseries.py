import logging
from uuid import UUID

import pandas as pd
import pymongo

import emission.core.get_database as edb
import emission.storage.timeseries.builtin_timeseries as bits

class AggregateTimeSeries(bits.BuiltinTimeSeries):
    def __init__(self):
        super(AggregateTimeSeries, self).__init__(None)
        self.user_query = _get_ignore_test_phone_extra_query()

    def _get_sort_key(self, time_query = None):
        return None

    def get_distinct_users(self, key_list= None, time_query=None,
                           geo_query=None, extra_query_list=None):
        query_to_use = self._get_query(key_list, time_query, geo_query,
                                       extra_query_list)
        logging.debug("query_to_use = %s" % query_to_use)
        return self.analysis_timeseries_db.find(query_to_use).distinct('user_id')


def _get_ignore_test_phone_extra_query():
    test_phone_ids = [UUID("079e0f1a-c440-3d7c-b0e7-de160f748e35"),
                      UUID("c76a0487-7e5a-3b17-a449-47be666b36f6"),
                      UUID("c528bcd2-a88b-3e82-be62-ef4f2396967a"),
                      UUID("95e70727-a04e-3e33-b7fe-34ab19194f8b"),
                      UUID("e471711e-bd14-3dbe-80b6-9c7d92ecc296"),
                      UUID("fd7b4c2e-2c8b-3bfa-94f0-d1e3ecbd5fb7"),
                      UUID("86842c35-da28-32ed-a90e-2da6663c5c73"),
                      UUID("3bc0f91f-7660-34a2-b005-5c399598a369")]

    test_phones_query = {"user_id": {"$nin": test_phone_ids}}
    return test_phones_query