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

class TestCommonTripQueries(unittest.TestCase):
    
    def setUp(self):
        self.testUserId = uuid.uuid4()
        self.testLocation = gj.Point((122.1234, 37.1234))
        self.testEnd = esdcpq.make_new_common_place(uuid.uuid4(), gj.Point((1,2.092)), ())
        self.testStart = esdcpq.make_new_common_place(uuid.uuid4(), gj.Point((1,2)), (self.testEnd.common_place_id,))
        self.time0 = datetime.datetime(1900, 1, 1, 1)
        edb.get_common_trip_db().drop()

    def tearDown(self):
        edb.get_common_trip_db().drop()

    def testCreation(self):
        place = esdcpq.make_new_common_place(self.testUserId, self.testLocation, ())
        print place.coords
        self.assertEqual(type(place.coords), gj.Point)
        #print "place.successors = %s" % place.successors
        print place.coords
        print "place.common_place_id = %s" % place.common_place_id
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
            self.assertTrue(len(place.successors) > 0)

    def testGetSuccessor(self):
        probs = np.zeros( (7, 24) )
        probs[3,3] = 10
        props = {
            "user_id" : self.testUserId,
            "start_loc" : self.testStart.common_place_id,
            "end_loc" : self.testEnd.common_place_id,
            "common_trip_id" : esdctp.make__id(self.testUserId, self.testStart.common_place_id, self.testEnd.common_place_id),
            "probabilites" : probs
        }
        trip = esdctp.make_common_trip(props)
        esdctp.save_common_trip(trip)
        suc = esdcpq.get_successor(self.testUserId, self.testStart, self.time0)
        self.assertEqual(suc, self.testEnd.common_place_id)


def get_fake_data(user_name):
    # Call with a username unique to your database
    tg.create_fake_trips(user_name)
    return eamtcp.main(user_name)

if __name__ == "__main__":
    unittest.main()