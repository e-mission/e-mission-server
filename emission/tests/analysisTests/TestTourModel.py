# Standard imports
import sys
import unittest
import datetime
import logging

# Our imports
from emission.analysis.modelling.tour_model.tour_model_matrix import Commute, TourModel, Location
from emission.core.wrapper.trip_old import Coordinate, Trip
from emission.simulation.trip_gen import create_fake_trips
from emission.analysis.modelling.tour_model.create_tour_model_matrix import create_tour_model
from emission.core.get_database import get_trip_db
import emission.analysis.modelling.tour_model.cluster_pipeline as eamtcp


class TestTourModel(unittest.TestCase):

    def setUp(self):
        time = datetime.datetime(2015, 4, 20, 0, 0, 0)
        self.our_tm = TourModel("test_user", 0, time)
        self.home = Location('home', self.our_tm)
        self.work = Location('work', self.our_tm)
        self.commute = Commute(self.home, self.work)
        self.our_tm.add_location(self.home, Coordinate(37.868360, -122.252857))
        self.our_tm.add_location(self.work, Coordinate(37.875715, -122.259049))
        self.our_tm.add_edge(self.commute)

    def tearDown(self):
        pass

    def testGetLocation(self):
        home_key = Location.make_lookup_key('home')
        work_key = Location.make_lookup_key('work')
        home_we_just_got = self.our_tm.get_location(home_key)
        work_we_just_got = self.our_tm.get_location(work_key)
        
        self.assertTrue(home_we_just_got == self.home)
        self.assertTrue(home_we_just_got is self.home)

        self.assertTrue(work_we_just_got == self.work)
        self.assertTrue(work_we_just_got is self.work)

        new_home = self.our_tm.get_location(self.home)
        self.assertTrue(new_home == self.home)
        self.assertTrue(new_home is self.home)

        new_work = self.our_tm.get_location(self.work)
        self.assertTrue(new_work == self.work)
        self.assertTrue(new_work is self.work)


    def testGetCommute(self):
        commute_key = Commute.make_lookup_key(self.home, self.work)
        self.assertEquals(commute_key, 'home->work')

        commute_we_just_got = self.our_tm.get_edge(self.home, self.work)
        self.assertTrue(commute_we_just_got == self.commute)
        self.assertTrue(commute_we_just_got is self.commute)

        self.assertTrue(commute_we_just_got == self.commute)
        self.assertTrue(commute_we_just_got is self.commute)


    def testWeight(self):
        dummy_trip = Trip(0, 0, 0, 0, 0, 0, self.home.rep_coords, self.work.rep_coords)
        self.assertEquals(self.commute.weight(), 0)
        self.commute.add_trip(dummy_trip)

        self.assertEquals(self.commute.weight(), 1)
        temp_com = self.our_tm.get_edge(self.home, self.work)
        self.assertEquals(temp_com.weight(), 1)

    def testSuccessorProbCount(self):
        self.home.increment_successor(self.work, 8, 3)
        self.assertEquals(self.commute.probabilities[3, 8], 1)
        self.assertEquals(self.commute.probabilities[1, 2], 0)

        self.commute.increment_prob(8, 3)
        com = self.our_tm.get_edge(self.home, self.work)
        self.assertEquals(com.probabilities[3, 8], 2)

    def testBasicGetSuccessor(self):
        self.home.increment_successor(self.work, 8, 0)
        self.assertTrue(self.home.hasSuccessor())
        self.assertTrue(self.home.get_successor()[0] == self.work)
        self.assertTrue(self.home.get_successor()[0] is self.work)
        self.assertTrue(self.home.get_successor()[1] == 8)
        self.assertFalse(self.work.hasSuccessor())

    def testGetTopTrips(self):
        dummy_trip0 = Trip(0, 0, 0, 0, 0, 0, self.home.rep_coords, self.work.rep_coords)
        dummy_trip1 = Trip(0, 0, 0, 0, 0, 1, self.home.rep_coords, self.work.rep_coords)
        dummy_trip2 = Trip(0, 0, 0, 0, 0, 2, self.home.rep_coords, self.work.rep_coords)
        self.commute.add_trip(dummy_trip0)
        self.commute.add_trip(dummy_trip1)
        self.commute.add_trip(dummy_trip2)

        coffee = Location('coffee', self.our_tm)
        tea = Location('tea', self.our_tm)
        dummy_trip3 = Trip(0, 0, 0, 0, 0, 3, coffee.rep_coords, tea.rep_coords)
        self.our_tm.add_location(coffee, Coordinate(4, 20))
        self.our_tm.add_location(tea, Coordinate(6, 9))
        commute2 = Commute(coffee, tea)
        commute2.add_trip(dummy_trip3)
        self.our_tm.add_edge(commute2)

        cheeseboard = Location('cheeseboard', self.our_tm)
        sliver = Location('sliver', self.our_tm)
        commute3 = Commute(cheeseboard, sliver)
        self.our_tm.add_location(cheeseboard, Coordinate(0, 0))
        self.our_tm.add_location(sliver, Coordinate(0, 0))
        self.our_tm.add_edge(commute3)

        self.assertTrue(self.our_tm.get_top_trips(1) == [self.commute])
        self.assertTrue(self.our_tm.get_top_trips(2) == [self.commute, commute2])
        self.assertTrue(self.our_tm.get_top_trips(3) == [self.commute, commute2, commute3])

    def testFirstTrip(self):
        self.home.increment_successor(self.work, 8, 0)
        t = datetime.datetime(2015, 4, 20, 0)
        self.our_tm.add_start_hour(self.home, t)
        t1 = datetime.datetime(2015, 4, 20, 6)
        place_after_home = Location('not home', t1)
        self.assertTrue(self.our_tm.min_of_each_day[0] == (self.home, t))

    def testRandomWalk(self):
        self.home.increment_successor(self.work, 8, 0)
        t = datetime.datetime(2015, 4, 20, 0)
        self.our_tm.add_start_hour(self.home, t)
        rw = self.our_tm.get_tour_model_for_day(0)
        self.assertEqual(rw, [self.home, self.work])

    def testCreation(self):
        # This is mostly just a sanity check
        db = get_trip_db()
        db.remove()
        create_fake_trips()
        list_of_cluster_data = eamtcp.main()
        tm = create_tour_model('test_user', list_of_cluster_data)
        # self.assertEquals(len(tm.get_top_trips(1)), 1)
        tour = tm.build_tour_model()
        # self.assertEquals(len(tour), 7)



if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    unittest.main()
