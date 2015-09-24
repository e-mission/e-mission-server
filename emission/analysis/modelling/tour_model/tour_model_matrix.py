# Our imports
import emission.simulation.markov_model_counter as esmmc
from emission.core.our_geocoder import Geocoder
import emission.core.get_database as edb

# Standard imports
import numpy as np
import math, datetime, heapq
import networkx as nx
import matplotlib.pyplot as plt

# Constants
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
        self.trips = [ ]

    def increment_prob(self, hour, day):
        self.probabilities[day, hour] += 1

    def set_predicted_mode(self, pred_mode):
        self.predicted_mode = pred_mode

    def get_predicted_mode(self):
        return self.predicted_mode

    def set_confirmed_mode(self, mode):
        self.confirmed_mode = mode

    def get_confirmed_mode(self):
        return self.confirmed_mode

    @classmethod
    def make_lookup_key(cls, starting_point, ending_point):
        return "%s->%s" % (starting_point, ending_point)

    def __repr__(self):
        return self.make_lookup_key(self.starting_point, self.ending_point)

    def add_trip(self, trip):
        self.trips.append(trip)

    def get_rough_time_duration(self):
        ## Based on a 40 mph guess
        dist = self.starting_point.rep_coords.distance(self.ending_point.rep_coords)
        miles = dist * 0.000621371192
        time = miles/float(40)
        if time < 1.0/2.0:
            time = 1.0/2.0   ## Pushes the random walk forward
        return datetime.timedelta(hours=time)

    def get_distance(self):
        return self.starting_point.rep_coords.distance(self.ending_point.rep_coords)

    def weight(self):
        return len(self.trips)

    def __eq__(self, other):
        return (self.starting_point == other.starting_point) and (self.ending_point == other.ending_point)

    def __ne__(self, other):
        return not self == other

    def _save_to_db(self):
        db = edb.get_commute_db()
        loc_list = [self.starting_point.name, self.ending_point.name]
        trip_list = [trip._id for trip in self.trips]
        str_array = np.array_str(self.probabilities)
        db.insert({"tm" : self.starting_point.tm.get_id(), "loc_list" : loc_list, "trip_list" : trip_list, 'probs' : str_array})

    @classmethod
    def build_from_json(cls, jsn_object):
        tm_id = jsn_object['tm']
        sp_name, ep_name = jsn_object['loc_list'][0], jsn_object['loc_list'][1]
        probs = jsn_object['probs']
        sp_json = edb.get_location_db().find_one({"name" : sp_name, "tm" : tm_id})
        ep_json = edb.get_location_db().find_one({"name" : ep_name, "tm" : tm_id})
        start = Location.build_from_json(sp_json, tm_id)
        end = Location.build_from_json(ep_json, tm_id)
        com = Commute(start, end)
        com.probabilities = np.array(np.mat(probs))
        return com

class Location(object):

    """ A node in the graph, lots of cool functions, will add more for units """

    def __init__(self, name, tour_model):
        self.tm = tour_model
        self.name = name
        self.successors = set( )
        self.edges = set( )
        self.rep_coords = None ## coordinates that represent the location
        self.address = None  ## Address that corresponds to rep_coords  

    def set_rep_coords(self, rep_coords):
        self.rep_coords = rep_coords

    def get_address(self):
        if self.rep_coords is None:
            raise Exception("You need to input representative coordinates first!")
        if self.address is None:
            geo = Geocoder()
            self.address = geo.reverse_geocode(self.rep_coords.get_lat(), self.rep_coords.get_lon())
        return self.address

    def increment_successor(self, suc, hour, day):
        the_edge = self.tm.get_edge(self, suc)
        the_edge.increment_prob(hour, day)
        suc_key = Location.make_lookup_key(suc.name)
        edge_key = Commute.make_lookup_key(self, suc)
        self.successors.add(self.tm.locs[suc_key])
        self.edges.add(self.tm.edges[edge_key])

    def get_successor(self):
        temp_counter = esmmc.Counter( )
        time = self.tm.time
        day = time.weekday()
        for suc in self.successors:
            suc_obj = self.tm.get_location(suc)
            edge = self.tm.get_edge(self, suc_obj)
            for temp_hour in xrange(time.hour, HOURS_IN_DAY):
                counter_key = (suc_obj, temp_hour, edge.get_rough_time_duration())
                temp_counter[counter_key] = edge.probabilities[day, temp_hour]
        return esmmc.sampleFromCounter(temp_counter)

    def hasSuccessor(self):
        day = self.tm.time.weekday()
        time = self.tm.time
        for suc in self.successors:
            suc_obj = self.tm.get_location(suc)
            edge = self.tm.get_edge(self, suc_obj)
            for temp_hour in xrange(time.hour, HOURS_IN_DAY):
                if edge.probabilities[day, temp_hour] > 0:
                    return True
        return False

    def get_in_degree(self):
        count = 0
        for loc in self.tm.locs.values():
            if loc in self.successors:
                count += 1
        return count

    def add_to_successors(self, successor):
        self.successors.add(successor)

    @classmethod
    def make_lookup_key(cls, name):
        return "At Location %s" % name 

    def __eq__(self, other):
        if type(other) != Location:
            return False
        return (self.name == other.name) or (self.rep_coords.distance(other.rep_coords) < 300)

    def __ne__(self, other):
        return not self == other

    def __repr__(self):
        return str(self.name)

    def __str__(self):
        return str(self.name)

    def _save_to_db(self):
        db = edb.get_location_db()
        db.insert({"name" : self.name, "rep_coords" : coord_list(self.rep_coords), "tm" : self.tm.get_id()})

    @classmethod
    def build_from_json(cls, jsn_object, tour_model):
        print "json object is : %s and type of json object is : %s" % (jsn_object, type(jsn_object))
        name = jsn_object['name']
        rep_coords = jsn_object['rep_coords']
        loc = Location(name, tour_model)
        loc.rep_coords = rep_coords
        return loc

class TourModel(object):

    """ 
    A class that represents a canconical week of travel for a user of e-mission. 
    ie the graph that store the location and commute classes 
    
    """

    def __init__(self, user, time):
        self.user = user
        self.edges = { }
        self.locs = { }
        self.min_of_each_day = [0]*DAYS_IN_WEEK  ## Stores (location, hour) pairs as a way to find the day's starting point
        self.time = time
        self.orig_time = time

    def get_id(self):
        return self.user

    def get_top_trips(self, n):
        # sort edges by weight 
        # return n most common trips
        edges_list = list(self.edges.values())
        return heapq.nlargest(n, edges_list, key=lambda v: v.weight())

    def define_locations(self):
        for loc in self.locs.itervalues():
            print "%s : %s" % (loc.name, loc.get_address())

    def get_prob_of_place_x_at_time_y_on_date_z(x, y, z):
        loc_key = Location.make_lookup_key(x)
        loc = self.get_location(loc_key)

    def add_location(self, location, coords=None):
        if coords is not None:
            location.set_rep_coords(coords)
        key = Location.make_lookup_key(location.name)
        self.locs[key] = location

    def add_start_hour(self, loc, time):
        day = time.weekday()
        if self.min_of_each_day[day] == 0:
            self.min_of_each_day[day] = (loc, time)
        else:
            if time < self.min_of_each_day[day][1]:
                self.min_of_each_day[day] = (loc, time)

    def get_tour_model_for_day(self, day, need_time=False):
        tour_model = [ ]
        if self.min_of_each_day[day] == 0:
            return NoData()
        curr_node = self.min_of_each_day[day][0]
        self.time = self.min_of_each_day[day][1]
        print "hour = %s | day = %s | place = %s" % (self.time.hour, self.time.weekday(), curr_node.name)
        if need_time:
            tour_model.append( (curr_node, self.time) )
        else:
            tour_model.append(curr_node)
        while curr_node.hasSuccessor():
            info = curr_node.get_successor()
            curr_node = info[0]
            self.time = datetime.datetime(self.time.year, self.time.month, self.time.day, hour=info[1], minute=self.time.minute) + info[2]
            if self.time.weekday() != day:
                break
            print "hour = %s | day = %s | place = %s" % (self.time.hour, self.time.weekday(), info[0].name)
            if curr_node != tour_model[-1]:
                if need_time:
                    tour_model.append( (curr_node, self.time) ) ## Time of the commute
                else:
                    tour_model.append(curr_node)
        return tour_model

    def get_location(self, location):
        if type(location) == str:
            return self.locs[location]
        key = Location.make_lookup_key(location.name)
        return self.locs[key]
 
    def build_tour_model(self, need_time=False):
        tour_model = [ ]
        for day in xrange(DAYS_IN_WEEK):
            tour_model.append(self.get_tour_model_for_day(day, need_time))
        return tour_model 

    def get_edge(self, starting_point, ending_point):
        key = Commute.make_lookup_key(starting_point, ending_point)
        if key not in self.edges.keys():
            self.add_edge(Commute(starting_point, ending_point))
        return self.edges[key]

    def add_edge(self, commute):
        key = Commute.make_lookup_key(commute.starting_point, commute.ending_point)
        self.edges[key] = commute

    def see_graph(self):
        pos = {}
        edge_colors = []
        node_sizes = []
        plt.clf()
        G = nx.MultiDiGraph()
        labels = { }
        for v in self.locs.values():
            G.add_node(v)
            pos[v] = (v.rep_coords.lon, v.rep_coords.lat)
            labels[v] = v.get_address()
        for e in self.edges.values():
            start = e.starting_point
            end = e.ending_point
            G.add_edge(start,end)
        for v in G.nodes():
            n = 0
            for e in G.in_edges(v):
                edge = self.get_edge(e[0], e[1])
                n += edge.weight()
            node_sizes.append(max(n,1)*30)
        G = nx.MultiGraph(G)
        nx.draw_networkx(G, pos, node_color='#00FF80', with_labels=True, node_size=node_sizes, width=3.5)
        plt.show()


    def save_to_db(self):
        db = edb.get_tm_db()
        for loc in self.locs.itervalues():
            loc._save_to_db()
        for com in self.edges.itervalues():
            com._save_to_db()
        db.insert({"tm" : self.get_id(), "time" : self.time})

    def __eq__(self, other):
        # Make something to compare
        com_list_self = list(self.edges.keys())
        com_list_self_sorted = sorted(com_list_self)
        com_list_other = list(other.edges.keys())
        com_list_other_sorted = sorted(com_list_other)

        loc_list_self = list(self.locs.keys())
        loc_list_self_sorted = sorted(loc_list_self)
        loc_list_other = list(other.locs.keys())
        loc_list_other_sorted = sorted(loc_list_other)

        return (com_list_other_sorted == com_list_self_sorted) and (loc_list_self_sorted == loc_list_other_sorted)

    def __ne__(self, other):
        return not self == other


class NoData(object):

    """ A class that represent a derth of data for a day """

    def __init__(self):
        pass

    def __repr__(self):
        return "No data for this day."

    def __len__(self):
        return 0

## These are utility functions

def coord_list(coords):
    return [coords.get_lat(), coords.get_lon()]

def create_lines_list(walk_for_one_day):
    lst = [ ]
    for location in walk_for_one_day:
        lst.append(coord_list(location.rep_coords))
    return lst

def is_weekday(day):
    return not (day == 5 or day == 6)
