from recommender.otp import OTP, PathNotFoundException
import random, math
from recommender.trip import Coordinate 
import json
import datetime
from OurGeocoder import ReverseGeocode, Geocode

class Address:

    ## This class exisists only for caching purposes
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
        self.a_to_b = set( )
        self.num_trips = None
        self.radius = None
        self.amount_missed = 0

    def get_starting_ending_points(self):
        city_file = open("trip_generator/input.json", "r") ## User (Naomi) specifies locations and radius they want
        jsn = json.load(city_file)
        self.num_trips = jsn["number of trips"]
        self.radius = int(jsn["radius"])
        for address in jsn["starting centroids"]:
            self.starting_points.extend(generate_random_locations_in_radius(Address(address), self.radius, self.num_trips/len(jsn["starting centroids"])))
        for address in jsn["ending centroids"]:
            self.ending_points.extend(generate_random_locations_in_radius(Address(address), self.radius, self.num_trips/len(jsn["starting centroids"])))
     
    def make_a_to_b(self):
        for _ in xrange(self.num_trips + self.radius*3):  ## Based on very rough estimate of how many of these end up in the ocean
            start_index = random.randint(0, len(self.starting_points) - 1)
            end_index = random.randint(0, len(self.ending_points) - 1)
            starting_point = self.starting_points[start_index]
            ending_point = self.ending_points[end_index]
            to_add = ( starting_point, ending_point )
            self.a_to_b.add(to_add)


    def get_trips_from_a_to_b(self):
        mode_tuple = ("CAR", "WALK", "BICYCLE", "TRANSIT")
        curr_time = datetime.datetime.now()
        curr_month = curr_time.month
        curr_year = curr_time.year
        curr_day = curr_time.day
        curr_hour = curr_time.hour
        curr_minute = curr_time.minute
        for t in self.a_to_b:
            mode = mode_tuple[random.randint(0, len(mode_tuple) - 1)] ## Unsophisticated mode choice
            try:
                otp_trip = OTP(t[0], t[1], mode, write_day(curr_month, curr_day, curr_year), write_time(curr_hour, curr_minute), True)
                alt_trip = otp_trip.turn_into_trip(0, 0, 0)   ## ids dont matter here 
                alt_trip.save_to_db()
            except PathNotFoundException:
                print "In the sea, skipping"
                self.amount_missed += 1

def geocode_address(address):
    if address.cord is None:
        # business_geocoder = Geocoder()
        # results = business_geocoder.geocode(address.text)
        g = Geocode(address)
        address.cord = g.get_coords()
    else:
        results = address.cord
    return Coordinate(results[0].coordinates[1], results[0].coordinates[0])

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
    radius_in_degrees = kilometers_to_degrees(radius)
    x_0 = crd.get_lon()
    y_0 = crd.get_lat()
    u = random.random()
    v = random.random()
    w = radius_in_degrees * math.sqrt(u)
    t = 2 * math.pi * v
    x = w * math.cos(t)
    y = w * math.sin(t)
    x = float(x) / float(math.cos(y_0))   # To account for Earth something 
    return Coordinate(x + x_0, y + y_0)


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

def sanity_check():
    c = create_fake_trips()
    print "amount missed = %s" % c.amount_missed 

    ##print "points = %s" % (c.points)
    #print "a to b = %s" % c.a_to_b


if __name__ == "__main__":
    sanity_check()