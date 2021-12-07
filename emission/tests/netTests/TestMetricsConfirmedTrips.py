import unittest
import logging
import arrow
import os
import json
import bson.json_util as bju
from datetime import datetime

import emission.core.get_database as edb
import emission.core.wrapper.localdate as ecwl

import emission.tests.common as etc
import emission.analysis.intake.cleaning.filter_accuracy as eaicf
import emission.storage.timeseries.format_hacks.move_filter_field as estfm

from emission.net.api import metrics

class TestMetrics(unittest.TestCase):
    def setUp(self):
        self.analysis_conf_path = \
            etc.set_analysis_config("analysis.result.section.key", "analysis/confirmed_trip")
        self._loadDataFileAndInputs("emission/tests/data/real_examples/shankari_2016-06-20")
        self.testUUID1 = self.testUUID
        self._loadDataFileAndInputs("emission/tests/data/real_examples/shankari_2016-06-21")
        self.testUUID2 = self.testUUID

        logging.info(
            "After loading, timeseries db size = %s" % edb.get_timeseries_db().estimated_document_count())
        self.jun_start_ts = arrow.get(datetime(2016,6,1), "America/Los_Angeles").timestamp
        self.jun_end_ts = arrow.get(datetime(2016,7,30), "America/Los_Angeles").timestamp
        self.jun_start_dt = ecwl.LocalDate.get_local_date(self.jun_start_ts, "America/Los_Angeles")
        self.jun_end_dt = ecwl.LocalDate.get_local_date(self.jun_end_ts, "America/Los_Angeles")

    def _loadDataFileAndInputs(self, dataFile):
        etc.setupRealExample(self, dataFile)
        self.entries = json.load(open(dataFile+".user_inputs"), object_hook = bju.object_hook)
        etc.setupRealExampleWithEntries(self)
        etc.runIntakePipeline(self.testUUID)

    def tearDown(self):
        self.clearRelatedDb()
        os.remove(self.analysis_conf_path)

    def clearRelatedDb(self):
        edb.get_timeseries_db().delete_many({"user_id": self.testUUID1})
        edb.get_analysis_timeseries_db().delete_many({"user_id": self.testUUID1})
        edb.get_pipeline_state_db().delete_many({"user_id": self.testUUID1})
        edb.get_timeseries_db().delete_many({"user_id": self.testUUID2})
        edb.get_analysis_timeseries_db().delete_many({"user_id": self.testUUID2})
        edb.get_pipeline_state_db().delete_many({"user_id": self.testUUID2})

    def testCountTimestampMetrics(self):
        met_result = metrics.summarize_by_timestamp(self.testUUID2,
                                                    self.jun_start_ts, self.jun_end_ts,
                                       'd', ['count'], True)
        logging.debug(met_result)

        self.assertEqual(list(met_result.keys()), ['aggregate_metrics', 'user_metrics'])
        user_met_result = met_result['user_metrics'][0]
        agg_met_result = met_result['aggregate_metrics'][0]
 
        self.assertEqual(len(user_met_result), 2)
        self.assertEqual([m.nUsers for m in user_met_result], [1, 1])
        self.assertEqual(user_met_result[0].local_dt.day, 21)
        self.assertEqual(user_met_result[0]["bike"], 2)
        self.assertEqual(user_met_result[1].local_dt.day, 22)
        self.assertEqual(user_met_result[1]["walk"], 2)
        # We are not going to make absolute value assertions about
        # the aggregate values since they are affected by other
        # entries in the database. However, because we have at least
        # data for two days in the database, the aggregate data
        # must be at least that much larger than the original data.
        self.assertEqual(len(agg_met_result), 3)
        # no overlap between users at the daily level
        # bunch of intermediate entries with no users since this binning works
        # by range
        self.assertEqual([m.nUsers for m in agg_met_result], [1,1,1])
        # If there are no users, there are no values for any of the fields
        # since these are never negative, it implies that their sum is zero
        self.assertTrue('shared_ride' not in agg_met_result[2] and
                         'bike' not in agg_met_result[2])

    def testPandasNaNHandlingAndWorkaround(self):
        # Pandas currently ignores NaN entries in groupby
        import pandas as pd
        import numpy as np

        test_df = pd.DataFrame({"id": [1,2,3,4,5,6],
            "mode_confirm": ["walk", "bike", "bike", "walk", np.NaN, np.NaN]})

        # Current pandas behavior ignores NaN
        orig_grouping = test_df.groupby("mode_confirm").groups
        self.assertEquals(list(orig_grouping.keys()), ["bike", "walk"])

        # workaround replaces NaN with "unknown"
        new_test_df = test_df.fillna("unknown")

        # Now we should not ignore NaN
        new_grouping = new_test_df.groupby("mode_confirm").groups
        self.assertEquals(list(new_grouping.keys()), ["bike", "unknown", "walk"])

    def testCountTimestampPartialMissingLabels(self):
        self.entries = json.load(open("emission/tests/data/real_examples/shankari_2016-07-22"), object_hook = bju.object_hook)
        etc.setupRealExampleWithEntries(self)
        etc.runIntakePipeline(self.testUUID2)
        # We group the entire year so we get partial labels
        met_result = metrics.summarize_by_timestamp(self.testUUID2,
                                                    self.jun_start_ts, self.jun_end_ts,
                                       'y', ['count'], True)
        logging.debug(met_result)

        self.assertEqual(list(met_result.keys()), ['aggregate_metrics', 'user_metrics'])
        user_met_result = met_result['user_metrics'][0]
        agg_met_result = met_result['aggregate_metrics'][0]

        self.assertEqual(len(user_met_result), 1)
        self.assertEqual([m.nUsers for m in user_met_result], [1])
        self.assertEqual(user_met_result[0].local_dt.day, 31)
        self.assertEqual(user_met_result[0]["bike"], 2)
        self.assertEqual(user_met_result[0]["walk"], 2)
        self.assertEqual(user_met_result[0]["unknown"], 3)
        # We are not going to make absolute value assertions about
        # the aggregate values since they are affected by other
        # entries in the database. However, because we have at least
        # data for two days in the database, the aggregate data
        # must be at least that much larger than the original data.
        self.assertEqual(len(agg_met_result), 1)
        # no overlap between users at the daily level
        # bunch of intermediate entries with no users since this binning works
        # by range
        self.assertEqual([m.nUsers for m in agg_met_result], [2])
        # If there are no users, there are no values for any of the fields
        # since these are never negative, it implies that their sum is zero
        self.assertTrue('unknown' in agg_met_result[0])
        self.assertEqual(agg_met_result[0]["unknown"], 5)

    def testCountLocalDateMetrics(self):
        met_result = metrics.summarize_by_local_date(self.testUUID,
                                                     ecwl.LocalDate({'year': 2016, 'month': 6}),
                                                     ecwl.LocalDate({'year': 2016, 'month': 6}),
                                                     'MONTHLY', ['count'], True)
        self.assertEqual(list(met_result.keys()), ['aggregate_metrics', 'user_metrics'])
        user_met_result = met_result['user_metrics'][0]
        agg_met_result = met_result['aggregate_metrics'][0]

        logging.debug(met_result)

        # local timezone means that we only have one entry
        self.assertEqual(len(user_met_result), 1)
        self.assertEqual(user_met_result[0].nUsers, 1)
        self.assertEqual(user_met_result[0]["walk"], 2)
        self.assertEqual(user_met_result[0]["bike"], 2)
        self.assertEqual(len(agg_met_result), 1)
        self.assertEqual(agg_met_result[0].nUsers, 2)
        self.assertGreaterEqual(agg_met_result[0]["shared_ride"], 2)
        self.assertGreaterEqual(agg_met_result[0]["walk"], 4)

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
