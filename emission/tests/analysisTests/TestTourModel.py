# Standard imports
import sys
import unittest

# Our imports
from emission.analysis.modelling.tour_model.tour_model import *

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
        I know this seems cumbersome, but no one will actually have to type all
        of this out, it will all be automated. 
        """
        # Monday 
        home_start_mon = Location("home", 0, 0)
        work_1_mon = Location("work_1", 8, 0)
        coffee_mon = Location("coffee", 10, 0)
        lunch_mon = Location("lunch", 12, 0)
        work_2_mon = Location("work", 13, 0)
        home_end_mon = Location("home", 18, 0) 
        home_start_mon.add_successors({work_1_mon : 1})
        work_1_mon.add_successors({coffee_mon : 2})
        coffee_mon.add_successors({lunch_mon : 3})
        lunch_mon.add_successors({work_2_mon : 4}) ## Completes the cycle
        work_2_mon.add_successors({home_end_mon : 100})

        # Tuesday 
        home_start_tues = Location("home", 0, 1)
        work_1_tues = Location("work_1", 8, 1)
        coffee_tues = Location("coffee", 10, 1)
        lunch_tues = Location("lunch", 12, 1)
        work_2_tues = Location("work", 13, 1)
        home_end_tues = Location("home", 18, 1) 
        home_start_tues.add_successors({work_1_tues : 1})
        work_1_tues.add_successors({coffee_tues : 2})
        coffee_tues.add_successors({lunch_tues : 3})
        lunch_tues.add_successors({work_2_tues : 4}) ## Completes the cycle
        work_2_tues.add_successors({home_end_tues : 100})

        mon = Day(0, home_start_mon)
        tues = Day(1, home_start_tues)

        days = [mon, tues]
        week = TourModel("naomi", days)
        tm_for_week = week.build_tour_model()
        monday = [home_start_mon, work_1_mon, coffee_mon, lunch_mon, work_2_mon, home_end_mon]
        tuesday = [home_start_tues, work_1_tues, coffee_tues, lunch_tues, work_2_tues, home_end_tues]
        self.assertEquals([monday, tuesday], tm_for_week)  # This can only play out one way, a good sanity check



    def complicated_tour(self):
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
