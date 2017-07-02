import logging
from uuid import UUID

import pandas as pd
import pymongo

import emission.core.get_database as edb
import emission.storage.timeseries.builtin_timeseries as bits

TEST_PHONE_IDS = [UUID("079e0f1a-c440-3d7c-b0e7-de160f748e35"), # SDB iphone 1
                  UUID("c76a0487-7e5a-3b17-a449-47be666b36f6"), # SDB iphone 2
                  UUID("c528bcd2-a88b-3e82-be62-ef4f2396967a"), # SDB iphone 3
                  UUID("95e70727-a04e-3e33-b7fe-34ab19194f8b"), # SDB iphone 4
                  UUID("e471711e-bd14-3dbe-80b6-9c7d92ecc296"), # SDB android 1
                  UUID("fd7b4c2e-2c8b-3bfa-94f0-d1e3ecbd5fb7"), # SDB android 2
                  UUID("86842c35-da28-32ed-a90e-2da6663c5c73"), # SDB android 3
                  UUID("3bc0f91f-7660-34a2-b005-5c399598a369"), # SDB android 4
                  UUID('248d8da2-9288-41b2-a0fe-29c1f2f01932'), # SDB android 5
                  UUID("70968068-dba5-406c-8e26-09b548da0e4b"), # ITU android 1
                  UUID("6561431f-d4c1-4e0f-9489-ab1190341fb7"), # ITU android 2
                  UUID("92cf5840-af59-400c-ab72-bab3dcdf7818"), # ITU android 3
                  UUID("93e8a1cc-321f-4fa9-8c3c-46928668e45d")] # ITU android 4

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
    test_phones_query = {"user_id": {"$nin": TEST_PHONE_IDS}}
    return test_phones_query