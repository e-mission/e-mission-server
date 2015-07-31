# Our imports
import emission.simulation.markov_model_counter as esmmc
import numpy as np
import math, datetime

DAYS_IN_WEEK = 7
HOURS_IN_DAY = 24
MODES_TO_NUMBERS = {"walking" : 0, "car" : 1, "train" : 2, "bart" : 3, "bike" : 4}
NUM_MODES = len(MODES_TO_NUMBERS)

def is_weekday(day):
    return not (day == 5 or day == 6)

class Commute(object):
    
    """ An edge in the graph """

    def __init__(self, starting_point, ending_point):
        self.probabilities = np.zeros((DAYS_IN_WEEK, HOURS_IN_DAY))
        self.starting_point = starting_point
        self.ending_point = ending_point
        self.trips = [ ]

    def increment_prob(self, hour, day):
        #print "increment_prob"
        self.probabilities[day, hour] += 1

    def dict_key(self):
        return "%s->%s" % (self.starting_point, self.ending_point)

    def __repr__(self):
        return self.dict_key()

    def add_trip(self, trip):
        self.trips.append(trip)

    def get_rough_time_duration(self):
        ## Based on a 40 mph guess
        dist = self.starting_point.rep_coords.distance(self.ending_point.rep_coords)
        miles = dist * 0.000621371192
        time = hours=miles/float(40)
        if time < 1.0/60.0:
            time = 1.0/60.0
        return datetime.timedelta(hours=time)

    def weight(self):
        return len(self.trips)

    def __gt__(self, other):
        return self.weight() > other.weight()

    def __lt__(self, other):
        return self.weight() < other.weight()

class Location(object):

    """ A node in the grpah """

    def __init__(self, name, tour_model):
        self.tm = tour_model
        self.name = name
        self.successors = set( )
        self.edges = set( )
        self.rep_coords = None ## coordinates that represent the location


    def set_rep_coords(self, rep_coords):
        self.rep_coords = rep_coords

    def increment_successor(self, suc, hour, day):
        edge = Commute(self, suc)
        the_edge = self.tm.get_edge(edge)
        the_edge.increment_prob(hour, day)
        suc_key = Location.make_lookup_key(suc.name)
        self.successors.add(suc_key)
        self.edges.add(str(edge))

    def get_successor(self):
        temp_counter = esmmc.Counter( )
        time = self.tm.time
        day = time.weekday()
        for suc in self.successors:
            suc_obj = self.tm.get_location(suc)
            commute = Commute(self, suc_obj)
            edge = self.tm.get_edge(commute)
            #print "hour is %s" % hour.hour
            for temp_hour in xrange(time.hour, HOURS_IN_DAY):
                counter_key = (suc_obj, temp_hour, edge.get_rough_time_duration())
                temp_counter[counter_key] = edge.probabilities[day, temp_hour]
                if edge.probabilities[day, temp_hour] > 0:
                    print "%s -> %d" % (counter_key, edge.probabilities[day, temp_hour])
                    #print temp_counter
        return esmmc.sampleFromCounter(temp_counter)

    def hasSuccessor(self):
        temp_counter = esmmc.Counter( )
        day = self.tm.time.weekday()
        time = self.tm.time
        #print "self.successors = %s" % self.successors
        for suc in self.successors:
            print suc
            suc_obj = self.tm.get_location(suc)
            commute = Commute(self, suc_obj)
            edge = self.tm.get_edge(commute)
            #print "hour is %s" % hour.hour
            for temp_hour in xrange(time.hour, HOURS_IN_DAY):
                if edge.probabilities[day, temp_hour] > 0:
                    return True
        return False

    @classmethod
    def make_lookup_key(cls, name):
        return "At Location %s" % name 

    def is_end_of_day(self):
        return self.tm.hour == HOURS_IN_DAY - 1

    def __eq__(self, other):
        if type(other) != Location:
            return False
        return (self.name == other.name)

    def __ne__(self, other):
        return not self == other

    def __repr__(self):
        return "At Location %s" % self.name

    def __str__(self):
        return "At Location %s" % self.name

class TourModel(object):

    """ 
    A class that represents a canconical week of travel for a user of e-mission. 
    ie the graph that store the location and commute classes 
    
    """

    def __init__(self, user, day, time):
        self.user = user
        self.edges = { }
        self.locs = { }
        self.min_of_each_day = [0]*DAYS_IN_WEEK  ## Stores (location, hour) pairs as a way to find the day's starting point
        self.time = time

    def get_top_trips(self, n):
        # sort edges by weight 
        # return n most common trips
        edges_list = list(self.edges)
        edges_list.sort()
        return edges_list[:n] 

    def add_location(self, location, is_start, coords):
        name = "%s" % location
        loc = Location(name, self)
        loc.set_rep_coords(coords)
        key = Location.make_lookup_key(loc.name)
        self.locs[key] = loc

    def add_start_hour(self, loc, time):
        day = time.weekday()
        if self.min_of_each_day[day] == 0:
            self.min_of_each_day[day] = (loc, time)
        else:
            if time < self.min_of_each_day[day][1]:
                self.min_of_each_day[day] = (loc, time)

    def get_tour_model_for_day(self, day):
        tour_model = [ ]
        if self.min_of_each_day[day] == 0:
            return "No data for this day"
        curr_node = self.min_of_each_day[day][0]
        self.time = self.min_of_each_day[day][1]
        tour_model.append(curr_node)
        while curr_node.hasSuccessor():
            info = curr_node.get_successor()
            curr_node = info[0]
            self.time = datetime.datetime(self.time.year, self.time.month, self.time.day, hour=info[1], minute=self.time.minute) + info[2] 
            print self.time
            if curr_node != tour_model[-1]:
                tour_model.append(curr_node)
        return tour_model

    def get_location(self, location):
        if type(location) == str:
            return self.locs[location]
        key = Location.make_lookup_key(location.name)
        return self.locs[key]
 
    def build_tour_model(self):
        tour_model = [ ]
        for day in xrange(DAYS_IN_WEEK):
            tour_model.append(self.get_tour_model_for_day(day))
        return tour_model 

    def get_edge(self, commute):
        key = commute.dict_key()
        if key in self.edges.keys():
            return self.edges[key]
        self.edges[key] = commute
        return self.edges[key]

    def see_graph(self):
        vertices = set()
        labels = {}
        import networkx as nx 
        import matplotlib.pyplot as plt
        G = nx.DiGraph()
        i = 0
        for v in self.locs.itervalues():
            G.add_node(v)
            labels[v] = v.name
            i += 1
        for e in self.edges.values():
            start = e.starting_point
            end = e.ending_point
            G.add_edge(start,end)
        pos=nx.spring_layout(G)
        nx.draw_networkx_nodes(G, pos)
        nx.draw_networkx_edges(G, pos)
        nx.draw_networkx_labels(G, pos, labels)
        plt.show()
