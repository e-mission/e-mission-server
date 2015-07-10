import unittest
from trip_generator.tour_model import Location, TourModel

class TestTourModel(unittest.Testcase):

	def test_something(self):
	    """ Basic Sanity Check """
	    home = Location("home")
	    work = Location("work")
	    coffee = Location("coffee")
	    home.is_home = True
	    home.add_to_successors(work)
	    work.add_to_successors(coffee)
	    tm = TourModel(home, "test")
	    tm_from_work = tm.get_tour_model_from(work)
	    should_be = [work, coffee]
	   	self.assertEqauls(tm_from_work, should_be)

	def test_case_one(self):
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
	    should_be = [work, coffee, lunch, work]
	    self.assertEqauls(tm_from_work, should_be)


	def test_case_two(self):
	    """ Work to coffee to work to lunch and then back to work; slightly more complex """
	    home = Location("home")
	    work = Location("work")
	    coffee = Location("coffee")
	    lunch = Location("lunch")
	    home.add_to_successors(work)
	    work.add_to_successors(coffee)
	    coffee.add_to_successors(work)
	    work.add_to_successors(lunch)
	    lunch.add_to_successors(work)
	    tm = TourModel(home, "work -> coffee -> work -> lunch -> work")
	    tm_from_work = tm.get_tour_model_from(work)
	    should_be = [work, coffee, work, lunch, work]
	    self.assertEqauls(tm_from_work, should_be)


	def test_case_three(self):
	    """ Testing multiple tour models """
	    ## Weekday schedule 
	    home = Location("home")
	    work = Location("work")
	    coffee = Location("coffee")
	    lunch = Location("lunch")
	    
	    home.add_to_successors(work)
	    work.add_to_successors(coffee)
	    coffee.add_to_successors(work)
	    work.add_to_successors(lunch)
	    lunch.add_to_successors(work)

	    ## Weekend schedule 
	    weekend_home = Location("weekend_home")
	    beach = Location("beach")
	    restaraunt = Location("restaraunt")
	    another_place = Location("another_place")

	    home.add_to_successors(weekend_home)
	    weekend_home.add_to_successors(beach)
	    beach.add_to_successors(restaraunt)
	    restaraunt.add_to_successors(weekend_home)

	    tm = TourModel(home, "work -> coffee -> work -> lunch -> work && weekend_home -> beach -> restaraunt -> weekend_home")
	    all_tms = tm.get_all_tour_models()
	    should_be = [[work, coffee, work, lunch, work], [weekend_home, beach, restaraunt, weekend_home]]
	    self.assertEqauls(all_tms, should_be)