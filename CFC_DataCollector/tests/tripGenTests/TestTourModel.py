import sys
sys.path.append("./../../trip_generator")
import unittest
from tour_model import Location, TourModel

class TestTourModel(unittest.TestCase):

    def test_simple(self):
        """ 
        Work to coffee to lunch and then back to work; A simple day
        CHECK -- Works
        """
        home = Location("home")
        work = Location("work")
        coffee = Location("coffee")
        lunch = Location("lunch")
        home.add_to_successors(work)
        work.add_to_successors(coffee)
        coffee.add_to_successors(lunch)
        lunch.add_to_successors(work) ## Completes the cycle
        tm = TourModel(home, "work to coffe to lunch to work")
        tm_from_work = tm.get_tour_model_from(work)
        print "test_case_one"
        print tm_from_work

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
        home.add_to_successors(home_successors)
        work.add_to_successors(work_successors)
        friend.add_to_successors(friend_successors)
        store.add_to_successors(store_successors)
        soccer.add_to_successors(soccer_successors)
        vegtables.add_to_successors(veg_successors)
        gas.add_to_successors(gas_successors)

        tm = TourModel(home, "complicted free state machine")

        print "ran complicted touir"

        print tm.get_tour_model_from(home)



if __name__ == "__main__":
    unittest.main()