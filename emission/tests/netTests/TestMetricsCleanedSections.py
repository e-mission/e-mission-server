from __future__ import unicode_literals
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import
from future import standard_library
standard_library.install_aliases()
from builtins import *
import unittest
import logging
import arrow
import os

import emission.core.get_database as edb
import emission.core.wrapper.localdate as ecwl

import emission.tests.common as etc

import emission.analysis.intake.cleaning.filter_accuracy as eaicf

import emission.storage.timeseries.format_hacks.move_filter_field as estfm

from emission.net.api import metrics

class TestMetrics(unittest.TestCase):
    def setUp(self):
        self.analysis_conf_path = \
            etc.set_analysis_config("analysis.result.section.key", "analysis/cleaned_section")
        etc.setupRealExample(self,
                             "emission/tests/data/real_examples/shankari_2015-aug-21")
        self.testUUID1 = self.testUUID
        etc.setupRealExample(self,
                             "emission/tests/data/real_examples/shankari_2015-aug-27")
        etc.runIntakePipeline(self.testUUID1)
        etc.runIntakePipeline(self.testUUID)
        logging.info(
            "After loading, timeseries db size = %s" % edb.get_timeseries_db().count())
        self.aug_start_ts = 1438387200
        self.aug_end_ts = 1441065600
        self.day_start_dt = ecwl.LocalDate.get_local_date(self.aug_start_ts, "America/Los_Angeles")
        self.day_end_dt = ecwl.LocalDate.get_local_date(self.aug_end_ts, "America/Los_Angeles")

    def tearDown(self):
        self.clearRelatedDb()
        os.remove(self.analysis_conf_path)

    def clearRelatedDb(self):
        edb.get_timeseries_db().delete_many({"user_id": self.testUUID})
        edb.get_analysis_timeseries_db().delete_many({"user_id": self.testUUID})
        edb.get_pipeline_state_db().delete_many({"user_id": self.testUUID})
        edb.get_timeseries_db().delete_many({"user_id": self.testUUID1})
        edb.get_analysis_timeseries_db().delete_many({"user_id": self.testUUID1})
        edb.get_pipeline_state_db().delete_many({"user_id": self.testUUID1})

    def testCountTimestampMetrics(self):
        met_result = metrics.summarize_by_timestamp(self.testUUID,
                                                    self.aug_start_ts, self.aug_end_ts,
                                       'd', ['count'], True)
        logging.debug(met_result)

        self.assertEqual(list(met_result.keys()), ['aggregate_metrics', 'user_metrics'])
        user_met_result = met_result['user_metrics'][0]
        agg_met_result = met_result['aggregate_metrics'][0]

        self.assertEqual(len(user_met_result), 2)
        self.assertEqual([m.nUsers for m in user_met_result], [1,1])
        self.assertEqual(user_met_result[0].local_dt.day, 27)
        self.assertEqual(user_met_result[1].local_dt.day, 28)
        self.assertEqual(user_met_result[0].ON_FOOT, 4)
        self.assertEqual(user_met_result[0].BICYCLING, 2)
        # Changed from 3 to 4 - investigation at
        # https://github.com/e-mission/e-mission-server/issues/288#issuecomment-242531798
        self.assertEqual(user_met_result[0].IN_VEHICLE, 4)
        # We are not going to make absolute value assertions about
        # the aggregate values since they are affected by other
        # entries in the database. However, because we have at least
        # data for two days in the database, the aggregate data
        # must be at least that much larger than the original data.
        self.assertEqual(len(agg_met_result), 8)
        # no overlap between users at the daily level
        # bunch of intermediate entries with no users since this binning works
        # by range
        self.assertEqual([m.nUsers for m in agg_met_result], [1,1,0,0,0,0,1,1])
        # If there are no users, there are no values for any of the fields
        # since these are never negative, it implies that their sum is zero
        self.assertTrue('ON_FOOT' not in agg_met_result[2] and
                         'BICYCLING' not in agg_met_result[2] and
                         'IN_VEHICLE' not in agg_met_result[2])


    def testCountLocalDateMetrics(self):
        met_result = metrics.summarize_by_local_date(self.testUUID,
                                                     ecwl.LocalDate({'year': 2015, 'month': 8}),
                                                     ecwl.LocalDate({'year': 2015, 'month': 9}),
                                                     'MONTHLY', ['count'], True)
        self.assertEqual(list(met_result.keys()), ['aggregate_metrics', 'user_metrics'])
        user_met_result = met_result['user_metrics'][0]
        agg_met_result = met_result['aggregate_metrics'][0]

        logging.debug(met_result)

        # local timezone means that we only have one entry
        self.assertEqual(len(user_met_result), 1)
        self.assertEqual(user_met_result[0].nUsers, 1)
        self.assertEqual(user_met_result[0].ON_FOOT, 6)
        self.assertEqual(user_met_result[0].BICYCLING, 4)
        self.assertEqual(user_met_result[0].IN_VEHICLE, 5)
        # We are not going to make assertions about the aggregate values since
        # they are affected by other entries in the database but we expect them
        # to be at least as much as the user values
        self.assertEqual(len(agg_met_result), 1)
        self.assertEqual(agg_met_result[0].nUsers, 2)
        self.assertGreaterEqual(agg_met_result[0].BICYCLING,
                                user_met_result[0].BICYCLING + 1) # 21s has one bike trip
        self.assertGreaterEqual(agg_met_result[0].ON_FOOT,
                                user_met_result[0].ON_FOOT + 3) # 21s has three bike trips
        self.assertGreaterEqual(agg_met_result[0].IN_VEHICLE,
                                user_met_result[0].IN_VEHICLE + 3) # 21s has three motorized trips

    def testCountNoEntries(self):
        # Ensure that we don't crash if we don't find any entries
        # Should return empty array instead
        # Unlike in https://amplab.cs.berkeley.edu/jenkins/job/e-mission-server-prb/591/
        met_result_ld = metrics.summarize_by_local_date(self.testUUID,
                                                     ecwl.LocalDate({'year': 2000}),
                                                     ecwl.LocalDate({'year': 2001}),
                                                     'MONTHLY', ['count'], True)
        self.assertEqual(list(met_result_ld.keys()), ['aggregate_metrics', 'user_metrics'])
        self.assertEqual(met_result_ld['aggregate_metrics'][0], [])
        self.assertEqual(met_result_ld['user_metrics'][0], [])

        met_result_ts = metrics.summarize_by_timestamp(self.testUUID,
                                                       arrow.get(2000,1,1).timestamp,
                                                       arrow.get(2001,1,1).timestamp,
                                                        'm', ['count'], True)
        self.assertEqual(list(met_result_ts.keys()), ['aggregate_metrics', 'user_metrics'])
        self.assertEqual(met_result_ts['aggregate_metrics'][0], [])
        self.assertEqual(met_result_ts['user_metrics'][0], [])

if __name__ == '__main__':
    import emission.tests.common as etc
    etc.configLogging()
    unittest.main()
