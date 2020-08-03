from __future__ import print_function
from __future__ import unicode_literals
from __future__ import division
from __future__ import absolute_import
# Standard imports
from future import standard_library
standard_library.install_aliases()
from builtins import *
import unittest
import json
import logging
from datetime import datetime, timedelta
import datetime as pydt
import arrow

# Our imports
import emission.core.get_database as edb
from emission.net.api import visualize
import emission.tests.common as etc
import emission.analysis.intake.segmentation.trip_segmentation as eaist
import emission.analysis.intake.segmentation.section_segmentation as eaiss
import emission.analysis.intake.cleaning.filter_accuracy as eaicf
import emission.storage.timeseries.format_hacks.move_filter_field as estfm
import emission.core.wrapper.motionactivity as ecwm
import emission.core.wrapper.localdate as ecwl


class TestVisualize(unittest.TestCase):
    def setUp(self):
        etc.setupRealExample(self,
                             "emission/tests/data/real_examples/shankari_2015-aug-27")
        # eaicf.filter_accuracy(self.testUUID)
        etc.runIntakePipeline(self.testUUID)
        # estfm.move_all_filters_to_data()
        logging.info(
            "After loading, timeseries db size = %s" % edb.get_timeseries_db().count())
        self.day_start_ts = 1440658800
        self.day_end_ts = 1440745200
        self.day_start_dt = ecwl.LocalDate.get_local_date(self.day_start_ts, "America/Los_Angeles")
        self.day_end_dt = ecwl.LocalDate.get_local_date(self.day_end_ts, "America/Los_Angeles")

        # If we don't delete the time components, we end up with the upper and
        # lower bounds = 0, which basically matches nothing.
        del self.day_start_dt['hour']
        del self.day_end_dt['hour']

        del self.day_start_dt['minute']
        del self.day_end_dt['minute']

        del self.day_start_dt['second']
        del self.day_end_dt['second']

    def tearDown(self):
        self.clearRelatedDb()

    def clearRelatedDb(self):
        edb.get_timeseries_db().delete_many({"user_id": self.testUUID})
        edb.get_analysis_timeseries_db().delete_many({"user_id": self.testUUID})


    def testUserCommutePopRouteLocalDate(self):
        user_points = visualize.range_mode_heatmap_local_date(self.testUUID,
                                          ["BICYCLING"],
                                          self.day_start_dt,
                                          self.day_end_dt, None)
        print(user_points['lnglat'])
        self.assertTrue(len(user_points['lnglat']) > 0)

    def testAggCommutePopRouteLocalDate(self):
        agg_points = visualize.range_mode_heatmap_local_date(None,
                                          ["BICYCLING"],
                                          self.day_start_dt,
                                          self.day_end_dt, None)
        self.assertTrue(len(agg_points['lnglat']) > 0)
        # I have to add test data with modes, I will do that tomorrow.

    def testUserCommutePopRouteTimestamp(self):
        user_points = visualize.range_mode_heatmap_timestamp(self.testUUID,
                                          ["BICYCLING"],
                                          self.day_start_ts,
                                          self.day_end_ts, None)
        self.assertTrue(len(user_points['lnglat']) > 0)

    def testAggCommutePopRouteTimestamp(self):
        agg_points = visualize.range_mode_heatmap_timestamp(None,
                                          ["BICYCLING"],
                                          self.day_start_ts,
                                          self.day_end_ts, None)
        self.assertTrue(len(agg_points['lnglat']) > 0)
        # I have to add test data with modes, I will do that tomorrow.


if __name__ == '__main__':
    import emission.tests.common as etc
    etc.configLogging()

    unittest.main()
