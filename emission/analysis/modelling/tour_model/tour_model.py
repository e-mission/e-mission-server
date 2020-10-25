from __future__ import unicode_literals
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import
# Our imports
from future import standard_library
standard_library.install_aliases()
from builtins import *
from builtins import object


## A File meant to represent tour models for individual users 
## A tour model is a week of each users most popular travel destinations 

# Monday 
# >>> home_start_mon = Location("home", 0, 0)
# >>> work_1_mon = Location("work_1", 8, 0)
# >>> coffee_mon = Location("coffee", 10, 0)
# >>> lunch_mon = Location("lunch", 12, 0)
# >>> work_2_mon = Location("work", 13, 0)
# >>> home_end_mon = Location("home", 18, 0) 
# >>> home_start_mon.add_successors({work_1_mon : 1})
# >>> work_1_mon.add_successors({coffee_mon : 2})
# >>> coffee_mon.add_successors({lunch_mon : 3})
# >>> lunch_mon.add_successors({work_2_mon : 4}) ## Completes the cycle
# >>> work_2_mon.add_successors({home_end_mon : 100})

# Tuesday 
# >>> home_start_tues = Location("home", 0, 1)
# >>> work_1_tues = Location("work_1", 8, 1)
# >>> coffee_tues = Location("coffee", 10, 1)
# >>> lunch_tues = Location("lunch", 12, 1)
# >>> work_2_tues = Location("work", 13, 1)
# >>> home_end_tues = Location("home", 18, 1) 
# >>> home_start_tues.add_successors({work_1_tues : 1})
# >>> work_1_tues.add_successors({coffee_tues : 2})
# >>> coffee_tues.add_successors({lunch_tues : 3})
# >>> lunch_tues.add_successors({work_2_tues : 4}) ## Completes the cycle
# >>> work_2_tues.add_successors({home_end_tues : 100})

# >>> mon = Day(0, home_start_mon)
# >>> tues = Day(1, home_start_tues)

# >>> days = [mon, tues]
# >>> week = TourModel("naomi", days)
# >>> tm_for_week = week.build_tour_model()

# I know this seems like alot, but you shouldnt really be typing any of this out by hand
# Maybe there is a better way to do this...

class Location(object):

    def __init__(self, name, hour, day):
        self.hour = hour ## An int 0-23 representing the hour 
        self.name = name ## The name of the place, important for equality
        self.day = day ## 0-6 Monday-Sunday
        self.counter = esmmc.Counter( ) ## Reps successors and probabilities of each one

    def add_successors(self, suc_dict):
        for loc, weight in suc_dict.items():
            if (loc.hour < self.hour) or (loc.day < self.day):
                raise Exception("You can not go backwards in time!")
            self.counter[loc] = weight

    def get_successor(self):
        return esmmc.sampleFromCounter(self.counter)

    def is_end(self):
        return self.counter.totalCount() == 0

    def __eq__(self, other):
        return (self.name == other.name) and (self.day == other.day)

    def __ne__(self, other):
        return not self == other

    def __repr__(self):
        return self.name 

    def __str__(self):
        return "At %s, at hour, %s on day %s" % (self.name, self.hour, self.day)

class Day(object):

    """ Represents a day of a tour, full of locations """

    def __init__(self, num_day, starting_point):
        self.day = num_day
        self.starting_point = starting_point

    def get_tour_model(self):
        tour_model = [ ]
        curr_node = self.starting_point
        tour_model.append(curr_node)
        while not curr_node.is_end():
            curr_node = curr_node.get_successor()
            tour_model.append(curr_node)
        return tour_model

class TourModel(object):

    """ A class that represents a canconical week of travel for a user of e-mission. """

    def __init__(self, user, days):
        self.user = user
        self.days = days

    def build_tour_model(self):
        tour_model = [ ]
        for day in self.days:
            tour_model.append(day.get_tour_model())
        return tour_model
