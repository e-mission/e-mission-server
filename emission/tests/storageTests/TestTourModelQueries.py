import unittest
import logging

import emission.storage.decorations.tour_model_queries as esdtmq
import emission.core.get_database as edb

import emission.tests.common as etc
import emission.analysis.intake.cleaning.filter_accuracy as eaicf
import emission.analysis.intake.segmentation.trip_segmentation as eaist
import emission.analysis.intake.segmentation.section_segmentation as eaiss

class TestTourModelQueries(unittest.TestCase):

    ## These are mostly just sanity checks because the details are tested in TestCommonPlaceQueries and TestCommonTripQueries

    def setUp(self):
        etc.setupRealExample(self, "emission/tests/data/real_examples/shankari_2015-aug-27")
        eaicf.filter_accuracy(self.testUUID)

    def tearDown(self):
        edb.get_timeseries_db().remove({"user_id": self.testUUID})
        edb.get_analysis_timeseries_db().remove({"user_id": self.testUUID})
        edb.get_common_trip_db().drop()
        edb.get_common_place_db().drop()

    def testE2E(self):
        eaist.segment_current_trips(self.testUUID)
        eaiss.segment_current_sections(self.testUUID)
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
    logging.basicConfig(level=logging.DEBUG)
    unittest.main()
