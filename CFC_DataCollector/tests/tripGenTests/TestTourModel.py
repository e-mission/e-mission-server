import sys
sys.path.append("./../../trip_generator")
import unittest
from tour_model_1 import *

class TestTourModel(unittest.TestCase):


    def test_utilities(self):

        home_start = Location("home", 0, 0)
        work_1 = Location("work_1", 8, 0)
        coffee = Location("coffee", 10, 0)
        lunch = Location("lunch", 12, 0)
        work_2 = Location("work", 13, 0)
        home_end = Location("home", 18, 0) 

        self.assertEquals(home_start, home_end)
        self.assertFalse(home_start == coffee)


    def test_simple(self):
        """ 
        Work to coffee to lunch and then back to work; A simple day
        CHECK -- Works
        """
        home_start = Location("home", 0, 0)
        work_1 = Location("work_1", 8, 0)
        coffee = Location("coffee", 10, 0)
        lunch = Location("lunch", 12, 0)
        work_2 = Location("work", 13, 0)
        home_end = Location("home", 18, 0) 
        home_start.add_successors({work_1 : 1})
        work_1.add_successors({coffee : 2})
        coffee.add_successors({lunch : 3})
        lunch.add_successors({work_2 : 4}) ## Completes the cycle
        work_2.add_successors({home_end : 100})

        monday = Day(0, home_start)
        print monday.get_tour_model()

        days = [monday, tuesday, ]

        week = TourModel("naomi", days)
        week.build_tour_model()

    def test_complicated_tour(self):
        ## Create locations
        home = Location('home')
        work = Location('work')
        friend = Location('friend')
        store = Location('store')
        soccer = Location('soccer')
        vegtables = Location('vegtables')
        gas = Location('gas')

        ## Set up successors 
        home_successors = {work : 10, friend : 3, store : 1}    
        work_successors = {soccer : 100, vegtables : 10, friend : 50}
        friend_successors = {vegtables: 1}
        store_successors = {gas : 2, home : 1}
        soccer_successors = {gas : 1, home : 3}
        veg_successors = {gas : 5, home : 73}
        gas_successors = {home : 1}

        ## Build Free State Machine
        home.add_to_successors(home_successors, "day", "week", 0)
        work.add_to_successors(work_successors, "day", "week", 0)
        friend.add_to_successors(friend_successors, "day", "week", 0)
        store.add_to_successors(store_successors, "day", "week", 0)
        soccer.add_to_successors(soccer_successors, "day", "week", 0)
        vegtables.add_to_successors(veg_successors, "day", "week", 0)
        gas.add_to_successors(gas_successors, "day", "week", 0)

        tm = TourModel(home, "complicted free state machine")

        print "ran complicted touir"

        print tm.get_tour_model_from(home, "day", "week", 0)



if __name__ == "__main__":
    unittest.main()