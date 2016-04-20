import unittest

import emission.storage.decorations.tour_model_queries as esdtmq
import emission.core.get_database as edb

class TestTourModelQueries(unittest.TestCase):

    ## These are mostly just sanity checks because the details are tested in TestCommonPlaceQueries and TestCommonTripQueries

    def setUp(self):
        edb.get_common_trip_db().drop()
        edb.get_section_new_db().drop()
        edb.get_trip_new_db().drop()

    def tearDown(self):
        edb.get_common_trip_db().drop()
        edb.get_section_new_db().drop()
        edb.get_trip_new_db().drop()

    def     testE2E(self):
        fake_user_id = "fake_user_id"
        esdtmq.make_tour_model_from_fake_data(fake_user_id)
        tm = esdtmq.get_tour_model(fake_user_id)
        self.assertTrue(len(tm["common_trips"]) > 0)
        self.assertTrue(len(tm["common_places"]) > 0)

    def testNoData(self):
        fake_user_id = "new_fake"
        esdtmq.make_tour_model_from_raw_user_data(fake_user_id)
        tm = esdtmq.get_tour_model(fake_user_id)
        self.assertTrue(len(tm["common_places"]) == 0)
        self.assertTrue(len(tm["common_trips"]) == 0)


if __name__ == "__main__":
    unittest.main()