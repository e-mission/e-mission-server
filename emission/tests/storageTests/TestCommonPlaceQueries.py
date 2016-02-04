import unittest
import uuid
import geojson as gj

import emission.storage.decorations.common_place_queries as esdcpq
import emission.analysis.modelling.tour_model.cluster_pipeline as eamtcp
import emission.simulation.trip_gen as tg

class TestCommonTripQueries(unittest.TestCase):
    
    def setUp(self):
        self.testUserId = uuid.uuid4()
        self.testLocation = gj.Point((122.1234, 37.1234))

    def testCreation(self):
        place = esdcpq.make_new_common_place(self.testUserId, self.testLocation)
        self.assertEqual(type(place.coords), gj.Point)
        #print "place.successors = %s" % place.successors
        print place.coords
        print "place.common_place_id = %s" % place.common_place_id
        self.assertIsNotNone(place.successors)
        self.assertIsNotNone(place.edges)

    # def testCreatePlace(self):
    #     data = get_fake_data("test1")
    #     esdcpq.create_places(data, "test1")


def get_fake_data(user_name):
    # Call with a username unique to your database
    tg.create_fake_trips(user_name)
    return eamtcp.main(user_name)


if __name__ == "__main__":
    unittest.main()