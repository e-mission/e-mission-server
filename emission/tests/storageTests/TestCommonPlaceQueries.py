import unittest
import uuid
import geojson as gj
import datetime
import numpy as np

import emission.storage.decorations.common_place_queries as esdcpq
import emission.analysis.modelling.tour_model.cluster_pipeline as eamtcp
import emission.simulation.trip_gen as tg
import emission.core.get_database as edb
import emission.storage.decorations.common_trip_queries as esdctp

class TestCommonPlaceQueries(unittest.TestCase):
    
    def setUp(self):
        edb.get_common_place_db().drop()
        edb.get_common_trip_db().drop()
        self.testUserId = uuid.uuid4()
        self.testLocation = gj.Point((122.1234, 37.1234))
        self.testEnd = esdcpq.make_new_common_place(uuid.uuid4(), gj.Point((1,2.092)), ())
        self.testStart = esdcpq.make_new_common_place(uuid.uuid4(), gj.Point((1,2)), (self.testEnd.common_place_id,))
        esdcpq.save_common_place(self.testEnd)
        esdcpq.save_common_place(self.testStart)
        self.time0 = datetime.datetime(1900, 1, 1, 1)

    def tearDown(self):
        edb.get_common_trip_db().drop()
        edb.get_common_place_db().drop()

    def testCreation(self):
        place = esdcpq.make_new_common_place(self.testUserId, self.testLocation, ())
        self.assertEqual(type(place.coords), gj.Point)
        self.assertEqual(type(place.successors), tuple)
        self.assertIsNotNone(place.successors)

    def testCreatePlace(self):
        data = get_fake_data("test1")
        esdcpq.create_places(data, "test1")
        places = esdcpq.get_all_common_places_for_user("test1")
        places_list = []
        for p in places:
            places_list.append(esdcpq.make_common_place(p))
        for place in places_list:
            self.assertIsNotNone(place.coords)
            self.assertIsNotNone(place["successors"])

    def testGetSuccessor(self):
        print "size of db is %s" % edb.get_common_place_db().find().count()
        self.assertIsNotNone(edb.get_common_place_db().find_one({"common_place_id": self.testEnd.common_place_id}))
        probs = np.zeros( (7, 24) )
        probs[self.time0.weekday(), 3] = 10
        props = {
            "user_id" : self.testUserId,
            "start_loc" : self.testStart.common_place_id,
            "end_loc" : self.testEnd.common_place_id,
            "probabilites" : probs,
            "trips" : ()
        }
        trip = esdctp.make_new_common_trip(props)
        esdctp.save_common_trip(trip)
        suc = esdcpq.get_succesor(self.testUserId, self.testStart.common_place_id, self.time0)
        self.assertEqual(suc, self.testEnd.common_place_id)


def get_fake_data(user_name):
    # Call with a username unique to your database
    tg.create_fake_trips(user_name, True)
    return eamtcp.main(user_name, False)

if __name__ == "__main__":
    unittest.main()