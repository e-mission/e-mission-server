from util import Counter, sampleFromCounter
import networkx as nx

DAYS_IN_WEEK = 7
HOURS_IN_DAY = 24

class Day(object):

    def __init__(self, instance):
        self.day = instance
        self.starting_point = None
        self.hours = [ ]

    def add_to_hours(self, hour):
        self.hours.append(hour)

    def set_starting_point(self, place):
        self.starting_point = place

    def starting_point_empty(self):
        return self.starting_point is None

    def get_hour(self, hour):
        return self.hours[hour]

    def get_starting_point(self):
        return self.starting_point


class Hour(object):

    """
    This class will represent some arbitrary unit of time that occurs on some repeated basis... 
    Like a day in a month or day in a week or hour in a day
    """

    def __init__(self, instance):
        self.inst = instance ## A number 0-23 military time style 
        self.counter = Counter( )


class Location(object):

    """ A class representing a location in a user's tour model. """

    def __init__(self, name):
        self.name = name
        self.successors = [ ]
        for d in xrange(DAYS_IN_WEEK):
            day = Day(d) 
            for h in xrange(HOURS_IN_DAY):
                hour = Hour(h)
                day.add_to_hours(hour)
            self.successors.append(day)

    def add_to_successors(self, loc, hour, day):
        if isinstance(loc, dict):
            day = Day(day)
            if day.starting_point_empty():
                day.set_starting_point(self)
            c = self.get_counter(hour, day.day)
            for location, w in loc.iteritems( ):
                c[location] = w

    def get_counter(self, hour, day):
        return self.successors[day].get_hour(hour).counter

    def __repr__(self):
        return self.name

    def __str__(self):
        return self.name

class TourModel(object):

    def __init__(self, home, name):
        self.home = home
        self.name = name
        self.starting_points = [ ]
        for day in xrange(DAYS_IN_WEEK):
            self.starting_points.append(None)

    def add_starting_point(self, place, day):
        self.starting_points[day] = place

    def get_tour_model_from(self, place, hour, day):
        tour_model = [ ]
        orig_place = place
        curr_node = place
        tour_model.append(curr_node)
        start = True
        while (curr_node != orig_place) or start:
            start = False
            curr_node = get_next_node(curr_node, hour, day)
            tour_model.append(curr_node)
        return tour_model

    def get_all_tour_models_for_week(self):
        temp = "place holder"
        tour_models = [ ]
        day = 0
        for sp in self.starting_points:
            tour_models.append(self.get_tour_model_from(sp, ))
        return tour_models


def get_next_node(place, hour, day):
    count = place.get_counter(hour, day)
    return sampleFromCounter(count)

def demo_alysia():

    ## Create Locations
    berkeley_home = Location('berkeley_home')
    CCH_Modesto = Location('CCH_Modesto')
    PTBE = Location('PTBE')
    San_Mateo_home = Location('San_Mateo_home')
    san_jose_high = Location('san_jose_high')
    wal_creek = Location('wal_creek')
    home = Location('home')
    work = Location('work')
    friend = Location('friend')
    store = Location('store')
    soccer = Location('soccer')
    vegtables = Location('vegtables')
    gas = Location('gas')

    ## Set up Successors 
    berkeley_home_sunday_successors = {CCH_Modesto : 50, berkeley_home : 1}
    CCH_Modesto_sunday_successors = {berkeley_home : 1}

    berkeley_home_wed_successors = {PTBE : 50, berkeley_home : 1}
    PTBE_wed_successors = {berkeley_home : 1}

    San_Mateo_home_thurs_succesors = {san_jose_high : 50, San_Mateo_home : 1} 
    san_jose_high_thurs_successors = {berkeley_home : 10}

    berkeley_home_friday_succesors = {wal_creek : 24, berkeley_home : 3}
    wal_creek_fri_suc = {berkeley_home : 1}

    home_successors = {work : 10, friend : 3, store : 1}    
    work_successors = {soccer : 100, vegtables : 10, friend : 50}
    friend_successors = {vegtables: 1}
    store_successors = {gas : 2, home : 1}
    soccer_successors = {gas : 1, home : 3}
    veg_successors = {gas : 5, home : 73}
    gas_successors = {home : 1}



    ## Build Free State Machine
    berkeley_home.add_to_successors(berkeley_home_sunday_successors, 0, 0)
    CCH_Modesto.add_to_successors(CCH_Modesto_sunday_successors, 0, 0)

    berkeley_home.add_to_successors(berkeley_home_wed_successors, 1, 3)
    PTBE.add_to_successors(PTBE_wed_successors, 1, 3)


    home.add_to_successors(home_successors, 10, 4)
    work.add_to_successors(work_successors, 10, 4)
    friend.add_to_successors(friend_successors, 10, 4)
    store.add_to_successors(store_successors, 10, 4)
    soccer.add_to_successors(soccer_successors, 10, 4)
    vegtables.add_to_successors(veg_successors, 10, 4)
    gas.add_to_successors(gas_successors, 10, 4)

    berkeley_home.add_to_successors(berkeley_home_friday_succesors, 10, 5)
    wal_creek.add_to_successors(wal_creek_fri_suc, 10, 5)


    tm = TourModel("alysia", berkeley_home)
    tm.add_starting_point(berkeley_home, 0)
    tm.add_starting_point(berkeley_home, 3)
    tm.add_starting_point(home, 4)
    tm.add_starting_point(berkeley_home, 5)
    print tm.get_all_tour_models_for_week()




