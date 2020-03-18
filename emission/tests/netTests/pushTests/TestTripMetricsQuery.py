from __future__ import unicode_literals
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import
from future import standard_library
standard_library.install_aliases()
from builtins import *
import unittest
import logging
import json
import bson.json_util as bju
import attrdict as ad
import arrow
import numpy as np

# Our imports
import emission.core.get_database as edb
import emission.core.wrapper.localdate as ecwl
import emission.core.wrapper.modestattimesummary as ecwm
import emission.core.wrapper.user as ecwu

import emission.net.usercache.abstract_usercache_handler as enuah
import emission.storage.timeseries.tcquery as estt

import emission.net.ext_service.push.query.trip_metrics as tripmetrics

# Test imports
import emission.tests.common as etc

class TestTripMetricsQuery(unittest.TestCase):
    def setUp(self):
        # Thanks to M&J for the number!
        np.random.seed(61297777)

    def tearDown(self):
        logging.debug("Clearing related databases")
        self.clearRelatedDb()

    def clearRelatedDb(self):
        edb.get_timeseries_db().delete_many({"user_id": {"$in": self.testUUIDList}})
        edb.get_analysis_timeseries_db().delete_many({"user_id": {"$in": self.testUUIDList}})
        edb.get_usercache_db().delete_many({"user_id": {"$in": self.testUUIDList}})
        edb.get_uuid_db().delete_many({"user_id": {"$in": self.testUUIDList}})

    def testGetMetricList(self):
        self.testUUIDList = []
        ml1 = tripmetrics.get_metric_list([{
                "modes": ['WALKING'],
                "metric": "distance",
                "threshold": {'$gt': 1000}
                }])
        self.assertEqual(ml1, ['distance'])

        ml2same = tripmetrics.get_metric_list([{
                "modes": ['WALKING'],
                "metric": "distance",
                "threshold": {'$gt': 1000}
                }, {
                "modes": ['WALKING', 'ON_FOOT'],
                "metric": "distance",
                "threshold": {'$lte': 100}
                }])
        self.assertEqual(ml2same, ['distance', 'distance'])

        ml2diff = tripmetrics.get_metric_list([{
                "modes": ['WALKING'],
                "metric": "distance",
                "threshold": {'$gt': 1000}
                }, {
                "modes": ['WALKING', 'ON_FOOT'],
                "metric": "count",
                "threshold": {'$lte': 10}
                }])
        self.assertEqual(ml2diff, ['distance', 'count'])

    def testCompareValue(self):
        self.testUUIDList = []
        self.assertEqual(tripmetrics.compare_value({"$gte": 100}, 100), True)
        self.assertEqual(tripmetrics.compare_value({"$gt": 100}, 100), False)
        self.assertEqual(tripmetrics.compare_value({"$lt": 100}, 100), False)
        self.assertEqual(tripmetrics.compare_value({"$lte": 200}, 100), True)
        self.assertEqual(tripmetrics.compare_value({"foo": 200}, 100), False)

    def testMatchesCheck(self):
        self.testUUIDList = []
        msts1 = ecwm.ModeStatTimeSummary({'nUsers': 1,
         'WALKING': 1, 'ON_FOOT': 2, 'BIKING': 1,
         'local_dt': {'hour': 0, 'month': 1, 'second': 0, 'weekday': 3, 'year': 2017, 'timezone': 'UTC', 'day': 19, 'minute': 0},
         'ts': 1484784000,
         'fmt_time': '2017-01-19T00:00:00+00:00'})
        checkSimpleNeg = {
          "modes": ['WALKING'],
          "metric": "count",
          "threshold": {'$gt': 4}
        }
        self.assertEqual(tripmetrics.matches_check(checkSimpleNeg, msts1), False)
        checkSimplePos = {
          "modes": ['WALKING'],
          "metric": "count",
          "threshold": {'$lt': 4}
        }
        self.assertEqual(tripmetrics.matches_check(checkSimplePos, msts1), True)
        checkComplexPos = {
          "modes": ['WALKING', 'ON_FOOT'],
          "metric": "count",
          "threshold": {'$gte': 3}
        }
        self.assertEqual(tripmetrics.matches_check(checkComplexPos, msts1), True)
        checkComplexNeg = {
          "modes": ['WALKING', 'ON_FOOT', 'BIKING'],
          "metric": "count",
          "threshold": {'$lte': 3}
        }
        self.assertEqual(tripmetrics.matches_check(checkComplexNeg, msts1), False)

        mstsNoData = ecwm.ModeStatTimeSummary({'nUsers': 0, 'local_dt': {'hour': 0, 'month': 1, 'second': 0, 'weekday': 2, 'year': 2017, 'timezone': 'UTC', 'day': 25, 'minute': 0}, 'ts': 1485302400, 'fmt_time': '2017-01-25T00:00:00+00:00'})

        # This is true because there are no entries, so the sum of the sections for the three modes is less than 3
        self.assertEqual(tripmetrics.matches_check(checkComplexNeg, mstsNoData), True)
        self.assertEqual(tripmetrics.matches_check(checkComplexPos, mstsNoData), False)

    def testIsMatchedUser(self):
        # Load data for the Bay Area
        dataFileba = "emission/tests/data/real_examples/shankari_2016-06-20"
        ldba = ecwl.LocalDate({'year': 2016, 'month': 6, 'day': 20})

        etc.setupRealExample(self, dataFileba)
        testUUIDba = self.testUUID
        etc.runIntakePipeline(testUUIDba)
        logging.debug("uuid for the bay area = %s " % testUUIDba)

        # Load data for Hawaii
        dataFilehi = "emission/tests/data/real_examples/shankari_2016-07-27"
        ldhi = ecwl.LocalDate({'year': 2016, 'month': 7, 'day': 27})

        etc.setupRealExample(self, dataFilehi)
        testUUIDhi = self.testUUID
        etc.runIntakePipeline(testUUIDhi)

        logging.debug("uuid for hawaii = %s " % testUUIDhi)

        self.testUUIDList = [testUUIDba, testUUIDhi]

        air_query_spec = {
            "time_type": "local_date",
            "from_local_date": { "year": 2016, "month": 2},
            "to_local_date": { "year": 2016, "month": 9},
            "freq": 'DAILY',
            "checks": [
                {
                    "modes": ['WALKING', 'ON_FOOT'],
                    "metric": "count",
                    "threshold": {"$gt": 5}
                },
                {
                    "modes": ['AIR_OR_HSR'],
                    "metric": "count",
                    "threshold": {"$gt": 1}
                }
            ]
        }

        # Since this requires at least one air trip, this will only return the
        # hawaii trip

        self.assertTrue(tripmetrics.is_matched_user(testUUIDhi, air_query_spec))
        self.assertFalse(tripmetrics.is_matched_user(testUUIDba, air_query_spec))
        
    def testQueryMatching(self):
        # Load data for the Bay Area
        dataFileba = "emission/tests/data/real_examples/shankari_2016-06-20"
        ldba = ecwl.LocalDate({'year': 2016, 'month': 6, 'day': 20})

        etc.setupRealExample(self, dataFileba)
        testUUIDba = self.testUUID
        edb.get_uuid_db().insert_one({"uuid": testUUIDba, "user_email": "sfbay@sfbay.location"})
        etc.runIntakePipeline(testUUIDba)
        logging.debug("uuid for the bay area = %s " % testUUIDba)

        # Load data for Hawaii
        dataFilehi = "emission/tests/data/real_examples/shankari_2016-07-27"
        ldhi = ecwl.LocalDate({'year': 2016, 'month': 7, 'day': 27})

        etc.setupRealExample(self, dataFilehi)
        testUUIDhi = self.testUUID
        edb.get_uuid_db().insert_one({"uuid": testUUIDhi, "user_email": "hawaii@hawaii.location"})
        etc.runIntakePipeline(testUUIDhi)

        logging.debug("uuid for hawaii = %s " % testUUIDhi)

        self.testUUIDList = [testUUIDba, testUUIDhi]

        air_query_spec = {
            "time_type": "local_date",
            "from_local_date": { "year": 2016, "month": 2},
            "to_local_date": { "year": 2016, "month": 9},
            "freq": 'DAILY',
            "checks": [
                {
                    "modes": ['WALKING', 'ON_FOOT'],
                    "metric": "count",
                    "threshold": {"$gt": 5}
                },
                {
                    "modes": ['AIR_OR_HSR'],
                    "metric": "count",
                    "threshold": {"$gt": 1}
                }
            ]
        }

        # Since this requires at least one air trip, this will only return the
        # hawaii trip
        self.assertEqual(tripmetrics.query(air_query_spec), [testUUIDhi])

        walk_drive_spec = {
            "time_type": "local_date",
            "from_local_date": { "year": 2016, "month": 2},
            "to_local_date": { "year": 2016, "month": 9},
            "freq": 'DAILY',
            "checks": [
                {
                    "modes": ['WALKING', 'ON_FOOT'],
                    "metric": "count",
                    "threshold": {"$gt": 5}
                },
                {
                    "modes": ['IN_VEHICLE'],
                    "metric": "count",
                    "threshold": {"$gt": 1}
                }
            ]
        }

        # Since this only requires walk and bike, will return both trips
        # We can't just do a simple equals check since the uuids may not always
        # be returned in the same order
        walk_drive_result = tripmetrics.query(walk_drive_spec)
        self.assertEqual(len(walk_drive_result), 2)
        self.assertIn(testUUIDhi, walk_drive_result)
        self.assertIn(testUUIDba, walk_drive_result)

if __name__ == '__main__':
    etc.configLogging()
    unittest.main()
