import unittest
import logging

import emission.core.get_database as edb
import emission.core.wrapper.localdate as ecwl

import emission.tests.common as etc

import emission.analysis.intake.cleaning.filter_accuracy as eaicf

import emission.storage.timeseries.format_hacks.move_filter_field as estfm
import emission.storage.decorations.local_date_queries as esdldq

from emission.net.api import metrics

class TestMetrics(unittest.TestCase):
    def setUp(self):
        etc.setupRealExample(self,
                             "emission/tests/data/real_examples/shankari_2015-aug-27")
        eaicf.filter_accuracy(self.testUUID)
        estfm.move_all_filters_to_data()
        logging.info(
            "After loading, timeseries db size = %s" % edb.get_timeseries_db().count())
        self.day_start_ts = 1440658800
        self.day_end_ts = 1440745200
        self.day_start_dt = esdldq.get_local_date(self.day_start_ts, "America/Los_Angeles")
        self.day_end_dt = esdldq.get_local_date(self.day_end_ts, "America/Los_Angeles")

    def tearDown(self):
        self.clearRelatedDb()

    def clearRelatedDb(self):
        edb.get_timeseries_db().remove({"user_id": self.testUUID})
        edb.get_analysis_timeseries_db().remove({"user_id": self.testUUID})

    def testCountTimestampMetrics(self):
        met_result = metrics.summarize_by_timestamp(self.testUUID,
                                       self.day_start_ts, self.day_end_ts,
                                       'd', 'metrics/daily_user_count')
        logging.debug(met_result)

        self.assertEqual(met_result.keys(), ['aggregate_metrics', 'user_metrics'])
        user_met_result = met_result['user_metrics']
        agg_met_result = met_result['aggregate_metrics']

        self.assertEqual(len(user_met_result), 2)
        self.assertEqual(user_met_result[0].local_dt.day, 27)
        self.assertEqual(user_met_result[1].local_dt.day, 28)
        self.assertEqual(user_met_result[0].ON_FOOT, 4)
        self.assertEqual(user_met_result[0].BICYCLING, 2)
        self.assertEqual(user_met_result[0].IN_VEHICLE, 3)
        # We are not going to make assertions about the aggregate values since
        # they are affected by other entries in the database but we expect them
        # to be at least as much as the user values
        self.assertGreaterEqual(agg_met_result[0].BICYCLING,
                                user_met_result[0].BICYCLING)

    def testCountLocalDateMetrics(self):
        met_result = metrics.summarize_by_local_date(self.testUUID,
                                                     ecwl.LocalDate({'year': 2015, 'month': 8}),
                                                     ecwl.LocalDate({'year': 2015, 'month': 9}),
                                                     'DAILY', 'metrics/daily_user_count')
        self.assertEqual(met_result.keys(), ['aggregate_metrics', 'user_metrics'])
        user_met_result = met_result['user_metrics']
        agg_met_result = met_result['aggregate_metrics']

        logging.debug(met_result)

        # local timezone means that we only have one entry
        self.assertEqual(len(user_met_result), 1)
        self.assertEqual(user_met_result[0].local_dt.day, 27)
        self.assertEqual(user_met_result[0].ON_FOOT, 6)
        self.assertEqual(user_met_result[0].BICYCLING, 4)
        self.assertEqual(user_met_result[0].IN_VEHICLE, 5)
        # We are not going to make assertions about the aggregate values since
        # they are affected by other entries in the database but we expect them
        # to be at least as much as the user values
        self.assertGreaterEqual(agg_met_result[0].BICYCLING,
                                user_met_result[0].BICYCLING)

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    unittest.main()
