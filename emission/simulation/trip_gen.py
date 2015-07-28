# Standard imports
import json
import datetime
from pygeocoder import Geocoder
import random, math

# Our imports
from emission.net.ext_service.otp.otp import OTP, PathNotFoundException
from emission.core.wrapper.trip import Coordinate 


class Address:

    ## This class exists only for caching purposes
    ## So we don't have to call google maps a million times

    def __init__(self, address):
        self.text = address
        self.cord = None
    
    def __str__(self):
        return self.text            

class Creator: 

    def __init__(self):
        self.starting_points = [ ]
        self.ending_points = [ ]
        self.a_to_b = [ ]
        self.num_trips = None
        self.radius = None
        self.amount_missed = 0
        self.starting_addresses = [ ]
        self.ending_addresses = [ ]
        self.labels = [ ]

    def get_starting_ending_points(self):
        city_file = open("emission/simulation/input.json", "r") ## User (Naomi) specifies locations and radius they want
        jsn = json.load(city_file)
        self.num_trips = jsn["number of trips"]
        self.radius = float(jsn["radius"])
        for address in jsn["starting centroids"]:            
            self.starting_points.extend(generate_random_locations_in_radius(Address(address), self.radius, self.num_trips/len(jsn["starting centroids"])))
            for i in range(self.num_trips/len(jsn["starting centroids"])):

                self.starting_addresses.append(address)
        for address in jsn["ending centroids"]:
            self.ending_points.extend(generate_random_locations_in_radius(Address(address), self.radius, self.num_trips/len(jsn["starting centroids"])))
            for i in range(self.num_trips/len(jsn["starting centroids"])):
                self.ending_addresses.append(address)

    def make_a_to_b(self):
        for _ in xrange(self.num_trips ):  ## Based on very rough estimate of how many of these end up in the ocean
            start_index = random.randint(0, len(self.starting_points) - 1)
            end_index = random.randint(0, len(self.ending_points) - 1)
            starting_point = self.starting_points[start_index]
            ending_point = self.ending_points[end_index]
            self.labels.append(str(self.starting_addresses[start_index]) + " to " + str(self.ending_addresses[end_index]))
            to_add = ( starting_point, ending_point )
            print to_add
            self.a_to_b.append(to_add)

    def get_trips_from_a_to_b(self):
        mode_tuple = ("CAR", "WALK", "BICYCLE", "TRANSIT")
        curr_time = datetime.datetime.now()
        curr_month = curr_time.month
        curr_year = curr_time.year
        curr_minute = curr_time.minute
        for i in range(len(self.a_to_b)):
            curr_day = random.randint(1, 28)
            curr_hour = random.randint(0, 23)
            t = self.a_to_b[i]
            print t
            mode = mode_tuple[random.randint(0, len(mode_tuple) - 1)] ## Unsophisticated mode choice, Alexi would throw up
            try:
                otp_trip = OTP(t[0], t[1], mode, write_day(curr_month, curr_day, curr_year), write_time(curr_hour, curr_minute), True)
                alt_trip = otp_trip.turn_into_trip(self.labels[i], 0, 0, True)   ## ids
                alt_trip.save_to_db()
            except PathNotFoundException:
                print "In the sea, skipping"
                self.amount_missed += 1

# def geocode_address(address):
#     print address.text
#     if address.cord is None:
#         g = Geocode()
#         address.cord = g.get_coords(address)
#         results = address.cord
#     else:
#         results = address.cord
#     return results

def geocode_address(address):
    if address.cord is None:
        business_geocoder = Geocoder()
        results = business_geocoder.geocode(address.text)
        address.cord = results
    else:
        results = address.cord
    return Coordinate(results[0].coordinates[0], results[0].coordinates[1])

def generate_random_locations_in_radius(address, radius, num_points):
    # Input the desired radius in kilometers
    print "num points is %s" % num_points
    locations = [ ]
    for _ in xrange(num_points):
        loc = get_one_random_point_in_radius(address, radius)
        locations.append(loc)
    return locations


def get_one_random_point_in_radius(address, radius):
    # From https://gis.stackexchange.com/questions/25877/how-to-generate-random-locations-nearby-my-location
    crd = geocode_address(address)
    print crd
    radius_in_degrees = kilometers_to_degrees(radius)
    x_0 = crd.get_lon()
    y_0 = crd.get_lat()
    u = random.random()
    v = random.random()
    w = radius_in_degrees * math.sqrt(u)
    t = 2 * math.pi * v
    x = w * math.cos(t)
    y = w * math.sin(t)
    print "type of y is %s" % type(y_0)
    print "x = %s" % x
    x = float(x) / float(math.cos(y_0))   # To account for Earth something 
    to_return = Coordinate(y + y_0, x + x_0)
    print to_return
    if abs(to_return.get_lat()) < 30 or abs(to_return.get_lon() < 30):
        print to_return.get_lon()
        print to_return.get_lat()

    return to_return

def kilometers_to_degrees(km):
    ## From stackexchnage mentioned above 
    return (float(km)/float(40000)) * 360

def write_day(month, day, year):
    return "%s-%s-%s" % (month, day, year)

def write_time(hour, minute):
    return "%s:%s" % (hour, minute) 


def create_fake_trips():
    ### Remember to fill in input.json!!!! 
    ### (This may not be the best way to do it. but for now it should be fine)
    my_creator = Creator()
    my_creator.get_starting_ending_points()
    my_creator.make_a_to_b()
    my_creator.get_trips_from_a_to_b()
    return my_creator

