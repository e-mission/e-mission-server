from __future__ import unicode_literals
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import
from future import standard_library
standard_library.install_aliases()
from builtins import *
import unittest
import logging

import emission.storage.decorations.tour_model_queries as esdtmq
import emission.core.get_database as edb

import emission.tests.common as etc
import emission.analysis.intake.cleaning.filter_accuracy as eaicf
import emission.analysis.intake.segmentation.trip_segmentation as eaist
import emission.analysis.intake.segmentation.section_segmentation as eaiss
import emission.analysis.intake.cleaning.clean_and_resample as eaicr

class TestTourModelQueries(unittest.TestCase):

    ## These are mostly just sanity checks because the details are tested in TestCommonPlaceQueries and TestCommonTripQueries

    def setUp(self):
        etc.setupRealExample(self, "emission/tests/data/real_examples/shankari_2015-aug-27")
        eaicf.filter_accuracy(self.testUUID)

    def tearDown(self):
        edb.get_timeseries_db().remove({"user_id": self.testUUID})
        edb.get_analysis_timeseries_db().remove({"user_id": self.testUUID})
        edb.get_timeseries_db().remove({"user_id": "new_fake"})
        edb.get_analysis_timeseries_db().remove({"user_id": "new_fake"})
        edb.get_common_trip_db().drop()
        edb.get_common_place_db().drop()

    def testE2E(self):
        etc.runIntakePipeline(self.testUUID)
        esdtmq.make_tour_model_from_raw_user_data(self.testUUID)
        tm = esdtmq.get_tour_model(self.testUUID)
        self.assertTrue(len(tm["common_trips"]) > 0)
        self.assertTrue(len(tm["common_places"]) > 0)

    def testNoData(self):
        fake_user_id = "new_fake"
        esdtmq.make_tour_model_from_raw_user_data(fake_user_id)
        tm = esdtmq.get_tour_model(fake_user_id)
        logging.debug("in testNoData, tour model = %s" % tm)
        self.assertTrue(len(tm["common_places"]) == 0)
        self.assertTrue(len(tm["common_trips"]) == 0)


if __name__ == "__main__":
    import emission.tests.common as etc
    etc.configLogging()
    unittest.main()
