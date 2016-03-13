import unittest
import uuid
import geojson as gj
import numpy as np
import datetime
import random

import emission.storage.decorations.common_trip_queries as esdctp
import emission.analysis.modelling.tour_model.cluster_pipeline as eamtcp
import emission.storage.decorations.common_place_queries as esdcpq
import emission.storage.decorations.trip_queries as esdtq
import emission.simulation.trip_gen as tg
import emission.core.get_database as edb
import emission.core.wrapper.trip as ecwt

class TestCommonTripQueries(unittest.TestCase):
    
    def setUp(self):
        edb.get_common_trip_db().drop()
        edb.get_section_new_db().drop()
        edb.get_trip_new_db().drop()
        self.testUserId = uuid.uuid4()
        self.testEnd = esdcpq.make_new_common_place(uuid.uuid4(), gj.Point((1,2.092)))
        self.testEnd.successors = ()
        self.testStart = esdcpq.make_new_common_place(uuid.uuid4(), gj.Point((1,2)))
        self.testStart = (self.testEnd.common_place_id,)
        self.time0 = datetime.datetime(1900, 1, 1, 1)

    def tearDown(self):
        edb.get_common_trip_db().drop()
        edb.get_section_new_db().drop()
        edb.get_trip_new_db().drop()

    def testCreation(self):
        common_trip = esdctp.make_new_common_trip()
        common_trip.user_id = self.testUserId
        common_trip.start_loc = self.testStart.user_id
        common_trip.end_loc = self.testEnd.user_id
        common_trip.trips = []
        self.assertIsNotNone(common_trip.start_loc)
        self.assertIsNotNone(common_trip.end_loc)
        self.assertIsNotNone(common_trip.trips)

    def testSaveAndRecieve(self):
        common_trip = esdctp.make_new_common_trip()
        common_trip.user_id = self.testUserId
        common_trip.start_loc = self.testStart.common_place_id
        common_trip.end_loc = self.testEnd.common_place_id
        common_trip.probabilites = np.zeros((24, 7))
        common_trip.trips = []
        esdctp.save_common_trip(common_trip)
        new_trip = esdctp.get_common_trip_from_db(self.testUserId, self.testStart.common_place_id, self.testEnd.common_place_id)
        self.assertEqual(new_trip.user_id, common_trip.user_id)
        self.assertEqual(new_trip.start_loc, common_trip.start_loc)
        self.assertEqual(new_trip.end_loc, common_trip.end_loc)

    def testCreateFromData(self):
        fake_data = get_fake_data("test2")
        esdctp.set_up_trips(fake_data, "test2")
        trips = esdctp.get_all_common_trips_for_user("test2")
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
    return eamtcp.main(user_name, old=False)


if __name__ == "__main__":
    unittest.main()
