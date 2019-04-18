from __future__ import unicode_literals
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import
from future import standard_library
standard_library.install_aliases()
from builtins import str
from builtins import *
import unittest
import uuid
import geojson as gj
import numpy as np
import datetime
import random
import logging

import emission.storage.decorations.common_trip_queries as esdctp
import emission.analysis.modelling.tour_model.cluster_pipeline as eamtcp
import emission.storage.decorations.common_place_queries as esdcpq
import emission.storage.decorations.trip_queries as esdtq
import emission.simulation.trip_gen as tg
import emission.core.get_database as edb
import emission.core.wrapper.trip as ecwt
import emission.tests.common as etc
import emission.analysis.intake.segmentation.trip_segmentation as eaist
import emission.analysis.intake.segmentation.section_segmentation as eaiss
import emission.analysis.intake.cleaning.filter_accuracy as eaicf
import emission.storage.timeseries.format_hacks.move_filter_field as estfm


class TestCommonTripQueries(unittest.TestCase):
    
    def setUp(self):
        # self.clearRelatedDb()
        self.testUserId = uuid.uuid4()
        self.testEnd = esdcpq.make_new_common_place(uuid.uuid4(), gj.Point((1,2.092)))
        esdcpq.save_common_place(self.testEnd)
        self.testEnd = esdcpq.get_common_place_at_location(self.testEnd.location)
        self.testEnd.successors = ()

        self.testStart = esdcpq.make_new_common_place(uuid.uuid4(), gj.Point((1,2)))
        self.testStart.successors = (self.testEnd.get_id(),)

        esdcpq.save_common_place(self.testEnd)
        esdcpq.save_common_place(self.testStart)
        self.time0 = datetime.datetime(1900, 1, 1, 1)


    def tearDown(self):
        edb.get_common_trip_db().remove({'user_id': self.testUserId})
        edb.get_analysis_timeseries_db().remove({'user_id': self.testUserId})
        edb.get_common_place_db().remove({'user_id': self.testUserId})

    def testCreation(self):
        common_trip = esdctp.make_new_common_trip()
        common_trip.user_id = self.testUserId
        common_trip.start_loc = self.testStart.location
        common_trip.end_loc = self.testEnd.location
        common_trip.trips = []
        self.assertIsNotNone(common_trip.start_loc)
        self.assertIsNotNone(common_trip.end_loc)
        self.assertIsNotNone(common_trip.trips)

    def testSaveAndRecieve(self):
        common_trip = esdctp.make_new_common_trip()
        common_trip.user_id = self.testUserId
        common_trip.start_place = self.testStart.get_id()
        common_trip.end_place = self.testEnd.get_id()
        common_trip.start_loc = self.testStart.location
        common_trip.end_loc = self.testEnd.location
        common_trip.probabilites = np.zeros((24, 7))
        common_trip.trips = []
        common_trip.start_times = [123456, 345789]
        common_trip.durations = [123,456,789]
        esdctp.save_common_trip(common_trip)
        new_trip = esdctp.get_common_trip_from_db(self.testUserId, self.testStart.get_id(), self.testEnd.get_id())
        self.assertEqual(new_trip.user_id, common_trip.user_id)
        self.assertEqual(gj.dumps(new_trip.start_loc), gj.dumps(common_trip.start_loc))
        self.assertEqual(gj.dumps(new_trip.end_loc), gj.dumps(common_trip.end_loc))

    def testCreateFromData(self):
        etc.setupRealExample(self, "emission/tests/data/real_examples/shankari_2015-aug-27")
        eaicf.filter_accuracy(self.testUUID)
        estfm.move_all_filters_to_data()
        eaist.segment_current_trips(self.testUUID)
        eaiss.segment_current_sections(self.testUUID)

        trips = esdctp.get_all_common_trips_for_user(self.testUUID)
        trips_list = []
        for p in trips:
            trips_list.append(esdctp.make_common_trip_from_json(p))
        for trip in trips_list:
            self.assertIsNotNone(trip.start_loc)
            self.assertIsNotNone(trip.end_loc)
            self.assertTrue(len(trip["trips"]) > 0)
            rand_id = random.choice(trip["trips"])
            self.assertEqual(type(esdtq.get_trip(rand_id)), ecwt.Trip) 
            self.assertTrue(trip.probabilites.sum() > 0)
            self.assertEqual(str(trip.user_id), "test2")



def get_fake_data(user_name):
    # Call with a username unique to your database
    tg.create_fake_trips(user_name, True)
    return eamtcp.main(user_name)


if __name__ == "__main__":
    import emission.tests.common as etc
    etc.configLogging()
    unittest.main()
