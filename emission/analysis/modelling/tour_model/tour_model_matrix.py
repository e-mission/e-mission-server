# Our imports
import emission.simulation.markov_model_counter as esmmc
import numpy as np

DAYS_IN_WEEK = 7
HOURS_IN_DAY = 24
MODES_TO_NUMBERS = {"walking" : 0, "car" : 1, "train" : 2, "bart" : 3, "bike" : 4}


NUM_MODES = len(MODES_TO_NUMBERS)


class Commute(object):
    
    """ An edge in the graph """

    def __init__(self, starting_point, ending_point):
        self.probabilities = np.zeros((DAYS_IN_WEEK, HOURS_IN_DAY))
        self.starting_point = starting_point
        self.ending_point = ending_point

    def increment_prob(self, hour, day):
        #print "increment_prob"
        self.probabilities[day, hour] += 1

    def dict_key(self):
        return "%s->%s" % (self.starting_point, self.ending_point)

    def __repr__(self):
        return self.dict_key()


class Location(object):

    """ A node in the grpah """

    def __init__(self, name, tour_model):
        self.tm = tour_model
        self.name = name
        self.successors = set( )
        self.edges = set( )

    def increment_successor(self, suc, hour, day):
        #print "INCREMENTING SUCCESSOR %s %s %s " % (hour, day, mode)
        edge = Commute(self, suc)
        the_edge = self.tm.get_edge(edge)
        the_edge.increment_prob(hour, day)
        self.successors.add(str(suc))
        self.edges.add(str(edge))

    def get_successor(self):
        temp_counter = esmmc.Counter( )
        day = self.tm.day
        hour = self.tm.time
        #print "self.successors = %s" % self.successors
        for suc in self.successors:
            suc_obj = self.tm.get_location(suc)
            commute = Commute(self, suc_obj)
            edge = self.tm.get_edge(commute)
            #print "hour is %s" % hour.hour
            for temp_hour in xrange(hour, HOURS_IN_DAY):
                counter_key = (suc_obj, temp_hour)
                temp_counter[counter_key] = edge.probabilities[day, temp_hour]
                if edge.probabilities[day, temp_hour] > 0:
                    print "%s -> %d" % (counter_key, edge.probabilities[day, temp_hour])

                    #print temp_counter
        return esmmc.sampleFromCounter(temp_counter)

    def hasSuccessor(self):
        temp_counter = esmmc.Counter( )
        day = self.tm.day
        hour = self.tm.time
        #print "self.successors = %s" % self.successors
        for suc in self.successors:
            suc_obj = self.tm.get_location(suc)
            commute = Commute(self, suc_obj)
            edge = self.tm.get_edge(commute)
            #print "hour is %s" % hour.hour
            for temp_hour in xrange(hour, HOURS_IN_DAY):
                counter_key = (suc_obj, temp_hour)
                temp_counter[counter_key] = edge.probabilities[day, temp_hour]
                    #print temp_counter
        return temp_counter.totalCount() != 0
            
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
        self.start_locs = { }
        self.end_locs = { } 
        self.min_of_each_day = [0, 0, 0, 0, 0, 0, 0]  ## Stores (location, hour) pairs as a way to find the day's starting point
        self.day = day
        self.time = time

    def add_location(self, location, is_start):
        name = "%s" % location
        loc = Location(name, self)
        if is_start:
            self.start_locs[str(loc)] = loc
        else:
            self.end_locs[str(loc)] = loc

    def add_start_hour(self, loc, hour, day):
        # print day
        # print "hour = %s, loc = %s" % (hour, loc)
        if self.min_of_each_day[day] == 0:
            self.min_of_each_day[day] = (loc, hour)
        else:
            if hour < self.min_of_each_day[day][1]:
                self.min_of_each_day[day] = (loc, hour)

    def get_tour_model_for_day(self, day):
        tour_model = [ ]
        curr_node = self.min_of_each_day[day][0]
        self.set_time(self.min_of_each_day[day][1])
        tour_model.append(curr_node)
        while curr_node.hasSuccessor():
            info = curr_node.get_successor()
            #print type(info)
            curr_node = info[0]
            self.set_time(info[1])
            tour_model.append(curr_node)
        return tour_model

    def set_time(self, time):
        self.time = time

    def set_day(self, day):
        self.day = day

    def get_location(self, location):
        #print "str(location) = %s" % str(location)
        if str(location) in self.start_locs.keys():
            return self.start_locs[str(location)]
        elif str(location) in self.end_locs.keys():
            return self.end_locs[str(location)]

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
        import networkx as nx 
        import matplotlib.pyplot as plt
        G = nx.DiGraph()
        for v in self.start_locs.values():
            G.add_node(v)
        for v in self.end_locs.values():
            G.add_node(v)
        for e in self.edges.values():
            start = e.starting_point
            end = e.ending_point
            G.add_edge(start,end)
        nx.draw(G)
        plt.show()
