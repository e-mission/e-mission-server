from util import Counter, sampleFromCounter

class Location(object):

    def __init__(self, name, hour, day):
        self.hour = hour ## An int 0-23 representing the hour 
        self.name = name
        self.day = day
        self.counter = Counter( ) ## Reps successors and probabilities of each one

    def add_successors(self, suc_dict):
        for loc, weight in suc_dict.iteritems():
            if (loc.hour < self.hour) or (loc.day < self.day):
                raise Exception("You can not go backwards in time!")
            self.counter[loc] = weight

    def get_successor(self):
        return sampleFromCounter(self.counter)

    def __eq__(self, other):
        return (self.name == other.name) and (self.day == other.day)

    def __ne__(self, other):
        return not self == other

    def __repr__(self):
        return self.name 

    def __str__(self):
        return "At %s, at hour, %s on day %s" % (self.name, self.hour, self.day)

class Day(object):

    """ Represents a day of a tour, full of hours """

    def __init__(self, num_day, starting_point):
        self.day = num_day
        self.starting_point = starting_point

    def get_tour_model(self):
        tour_model = [ ]
        curr_node = self.starting_point
        tour_model.append(curr_node)
        start = True
        while (curr_node != self.starting_point) or start:
            start = False
            curr_node = curr_node.get_successor()
            print curr_node.counter
            print curr_node
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

