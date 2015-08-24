# Standard imports
import random, math, json, datetime

# Our imports
from emission.net.ext_service.otp.otp import OTP, PathNotFoundException
from emission.core.wrapper.trip import Coordinate 
import emission.simulation.markov_model_counter as esmmc
from emission.core.our_geocoder import Geocoder

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
        self.starting_counter = esmmc.Counter( )
        self.ending_counter = esmmc.Counter( )
        self.mode_counter = esmmc.Counter( )
        self.prog_bar = ""

    def set_up(self):
        city_file = open("emission/simulation/input.json", "r") ## User (Naomi) specifies locations and radius they want
        jsn = json.load(city_file)
        self.num_trips = jsn["number of trips"]
        self.radius = float(jsn["radius"])
        for place, weight in jsn['starting centroids'].iteritems():
            self.starting_counter[Address(place)] = weight
        for place, weight in jsn['ending centroids'].iteritems():
            self.ending_counter[Address(place)] = weight
        for mode, weight in jsn['modes'].iteritems():
            self.mode_counter[mode] = weight
        city_file.close()

    def get_starting_ending_points(self):
        for _ in xrange(self.num_trips):
            start_addr = esmmc.sampleFromCounter(self.starting_counter)
            end_addr = esmmc.sampleFromCounter(self.ending_counter)
            self.starting_points.append(get_one_random_point_in_radius(start_addr, self.radius))
            self.ending_points.append(get_one_random_point_in_radius(end_addr, self.radius))

    def make_a_to_b(self):
        for _ in xrange(self.num_trips):  ## Based on very rough estimate of how many of these end up in the ocean
            start_index = random.randint(0, len(self.starting_points) - 1)
            end_index = random.randint(0, len(self.ending_points) - 1)
            starting_point = self.starting_points[start_index]
            ending_point = self.ending_points[end_index]
            to_add = ( starting_point, ending_point )
            self.a_to_b.append(to_add)

    def get_trips_from_a_to_b(self):
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
                print self.prog_bar
                rand_trip_id = random.random()
                rand_user_id = random.random()
                otp_trip = OTP(t[0], t[1], mode, write_day(curr_month, curr_day, curr_year), write_time(curr_hour, curr_minute), True)
                alt_trip = otp_trip.turn_into_trip("%f%f" % (rand_user_id, rand_trip_id), rand_user_id, rand_trip_id, True)   ## ids
                alt_trip.save_to_db()
            except PathNotFoundException:
                self.amount_missed += 1

def geocode_address(address):
    if address.cord is None:
        business_geocoder = Geocoder()
        results = business_geocoder.geocode(address.text)
        address.cord = results
    else:
        results = address.cord
    return results

def generate_random_locations_in_radius(address, radius, num_points):
    # Input the desired radius in kilometers
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
    x = float(x) / float(math.cos(y_0)) # To account for Earth curvature stuff
    to_return = Coordinate(y + y_0, x + x_0)
    return to_return

def kilometers_to_degrees(km):
    ## From stackexchnage mentioned above 
    return (float(km)/float(40000)) * 360

def write_day(month, day, year):
    return "%s-%s-%s" % (month, day, year)

def write_time(hour, minute):
    return "%s:%s" % (hour, minute) 

def create_fake_trips():
    ### This is the main function, its the only thing you need to run
    my_creator = Creator()
    my_creator.set_up()
    my_creator.get_starting_ending_points()
    my_creator.make_a_to_b()
    my_creator.get_trips_from_a_to_b()
    return my_creator

if __name__ == "__main__":
    create_fake_trips()
