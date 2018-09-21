from __future__ import print_function
from __future__ import division
from __future__ import unicode_literals
from __future__ import absolute_import
# Standard imports
from future import standard_library
standard_library.install_aliases()
from builtins import range
from builtins import *
from past.utils import old_div
from builtins import object
import random 
import math 
import json 
import datetime 
import urllib.request, urllib.error, urllib.parse
import sys

# Our imports
from emission.net.ext_service.otp.otp import OTP, PathNotFoundException
from emission.core.wrapper.trip_old import Coordinate 
import emission.simulation.markov_model_counter as esmmc
import emission.net.ext_service.geocoder.nominatim as enn
import emission.core.get_database as edb
import emission.core.wrapper.trip as ecwt
import emission.core.wrapper.section as ecws

class Address(object):

    ## This class exists only for caching purposes
    ## So we don't have to call google maps a million times

    def __init__(self, address):
        self.text = address
        self.cord = None
    
    def __str__(self):
        return self.text

    def __eq__(self, other):
        return (self.text.lower(), self.cord) == (other.text.lower(), self.cord)

    def __lt__(self, other):
        return (self.text.lower(), self.cord) < (other.text.lower(), other.cord)
    
    def __hash__(self):
        return hash((self.text, self.cord))

class Creator(object): 

    def __init__(self, new=False):
        self.new = new
        self.starting_points = [ ]
        self.ending_points = [ ]
        self.a_to_b = [ ]
        self.num_trips = None
        self.radius = None
        self.amount_missed = 0
        self.starting_counter = esmmc.Counter()
        self.ending_counter = esmmc.Counter()
        self.mode_counter = esmmc.Counter()
        self.prog_bar = ""

    def set_up(self):
        city_file = open("emission/simulation/input.json", "r") ## User (Naomi) specifies locations and radius they want
        jsn = json.load(city_file)
        self.num_trips = jsn["number of trips"]
        self.radius = float(jsn["radius"])
        for place, weight in jsn['starting centroids'].items():
            self.starting_counter[Address(place)] = weight
        for place, weight in jsn['ending centroids'].items():
            self.ending_counter[Address(place)] = weight
        for mode, weight in jsn['modes'].items():
            self.mode_counter[mode] = weight
        city_file.close()

    def get_starting_ending_points(self):
        for _ in range(self.num_trips):
            start_addr = esmmc.sampleFromCounter(self.starting_counter)
            end_addr = esmmc.sampleFromCounter(self.ending_counter)
            self.starting_points.append(get_one_random_point_in_radius(start_addr, self.radius))
            self.ending_points.append(get_one_random_point_in_radius(end_addr, self.radius))

    def make_a_to_b(self):
        for _ in range(self.num_trips):  ## Based on very rough estimate of how many of these end up in the ocean
            start_index = random.randint(0, len(self.starting_points) - 1)
            end_index = random.randint(0, len(self.ending_points) - 1)
            starting_point = self.starting_points[start_index]
            ending_point = self.ending_points[end_index]
            to_add = ( starting_point, ending_point )
            self.a_to_b.append(to_add)

    def get_trips_from_a_to_b(self, user_id):
        curr_time = datetime.datetime.now()
        curr_month = curr_time.month
        curr_year = curr_time.year
        curr_minute = curr_time.minute
        for i in range(len(self.a_to_b)):
            curr_day = random.randint(1, 28)
            curr_hour = random.randint(0, 23)
            t = self.a_to_b[i]
            mode = esmmc.sampleFromCounter(self.mode_counter) ## Unsophisticated mode choice, Alexi would throw up
            try:
                self.prog_bar += "."
                print(self.prog_bar)
                rand_trip_id = random.random()
                rand_user_id = user_id if user_id else random.random()
                otp_trip = OTP(t[0], t[1], mode, write_day(curr_month, curr_day, curr_year), write_time(curr_hour, curr_minute), True)
                if self.new:
                    print("here")
                    otp_trip.turn_into_new_trip(user_id)
                else:
                    alt_trip = otp_trip.turn_into_trip("%s%s" % (rand_user_id, rand_trip_id), rand_user_id, rand_trip_id, True)   ## ids
                    save_trip_to_db(alt_trip)
            except PathNotFoundException:
                print("path not found")
                self.amount_missed += 1
            except urllib.error.HTTPError:
                print("server error")
                pass   

def save_trip_to_db(trip):
    print("saving trip to db")
    print(trip.user_id)
    db = edb.get_trip_db()
    print("RHRHRH")
    print("start loc = %s" % trip.trip_start_location.coordinate_list())
    print("end loc = %s" % trip.trip_end_location.coordinate_list())
    db.insert({"_id": trip._id, "user_id": trip.user_id, "trip_id": trip.trip_id, "type" : "move", "sections": list(range(len(trip.sections))), "trip_start_datetime": trip.start_time.datetime,
            "trip_end_datetime": trip.end_time.datetime, "trip_start_location": trip.trip_start_location.coordinate_list(), 
            "trip_end_location": trip.trip_end_location.coordinate_list(), "mode_list": trip.mode_list})
    print("len(trip.sections) in trip gen is %s" % len(trip.sections))
    for section in trip.sections:   
        save_section_to_db(section)

def save_section_to_db(section):
    print("saving section to db")
    db = edb.get_section_db()
    db.insert({"user_id" : section.user_id, "trip_id" : section.trip_id, "distance" : section.distance, "type" : section.section_type,
           "section_start_datetime" : section.start_time.datetime, "section_end_datetime" : section.end_time.datetime, 
           "section_start_point" : {"coordinates" : section.section_start_location.coordinate_list()},
           "section_end_point" : {"coordinates" : section.section_end_location.coordinate_list()}, "mode" : section.mode, "confirmed_mode" : section.confirmed_mode})

def geocode_address(address):
    if address.cord is None:
        business_geocoder = enn.Geocoder()
        results = business_geocoder.geocode(address.text)
        address.cord = results
    else:
        results = address.cord
    return results

def generate_random_locations_in_radius(address, radius, num_points):
    # Input the desired radius in kilometers
    locations = [ ]
    for _ in range(num_points):
        loc = get_one_random_point_in_radius(address, radius)
        locations.append(loc)
    return locations

def get_one_random_point_in_radius(address, radius):
    # From https://gis.stackexchange.com/questions/25877/how-to-generate-random-locations-nearby-my-location
    crd = geocode_address(address)
    radius_in_degrees = kilometers_to_degrees(radius)
    x_0 = crd.get_lon()
    y_0 = crd.get_lat()
    u = random.random()
    v = random.random()
    w = radius_in_degrees * math.sqrt(u)
    t = 2 * math.pi * v
    x = w * math.cos(t)
    y = w * math.sin(t)
    x = old_div(float(x), float(math.cos(y_0))) # To account for Earth curvature stuff
    to_return = Coordinate(y + y_0, x + x_0)
    return to_return

def kilometers_to_degrees(km):
    ## From stackexchnage mentioned above 
    return (old_div(float(km),float(40000))) * 360

def write_day(month, day, year):
    return "%s-%s-%s" % (month, day, year)

def write_time(hour, minute):
    return "%s:%s" % (hour, minute) 

def create_fake_trips(user_name=None, new=False):
    ### This is the main function, its the only thing you need to run
    my_creator = Creator(new)
    my_creator.set_up()
    my_creator.get_starting_ending_points()
    my_creator.make_a_to_b()
    my_creator.get_trips_from_a_to_b(user_name)
    return my_creator


if __name__ == "__main__":
    user_id = sys.argv[1]
    create_fake_trips(user_id, True)
