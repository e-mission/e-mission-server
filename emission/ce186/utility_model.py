import emission.simulation.markov_model_counter as emmc
import emission.net.ext_service.otp.otp as otp
import emission.core.our_geocoder as geo
import emission.core.wrapper.trip_old as to

import datetime
import random

class UserBase:

    def __init__(self):
        self.users = {}
        self.crowd_areas = {}

    def add_user(self, user):
        self.users[user.name] = user

    def get_user(self, user_name):
        return self.users[user_name]

    def add_crowd(self, area):
        self.crowd_areas[area.name] = area

    def get_crowd_info(self, area_name):
        return self.crowd_areas[area_name]



class UserModel:

    def __init__(self, name, user_base, has_bike=False):
        self.name = name
        self.utilities = emmc.Counter()
        self.has_bike = has_bike
        self.user_base = user_base

        user_base.add_user(self)

        ## Initialize utilities
        self.utilities["sweat"] = 1
        self.utilities["scenery"] = 1
        self.utilities["social"] = 1
        self.utilities["time"] = 1
        self.utilities["noise"] = 1
        self.utilities["crowded"] = 1

    def get_top_choice_places(self, start_place, end_place):
        our_geo = geo.Geocoder()
        start = our_geo.geocode(start_place)
        end = our_geo.geocode(end_place)
        return self.get_top_choice_lat_lng(start, end)

    def get_top_choice_lat_lng(self, start, end, curr_time=None):
        if curr_time is None:
            curr_time = datetime.datetime.now()
        curr_month = curr_time.month
        curr_year = curr_time.year
        curr_minute = curr_time.minute
        curr_day = curr_time.day
        curr_hour = curr_time.hour
        mode = "WALK"
        if self.has_bike:
            mode = "BICYCLE"

        our_geo = geo.Geocoder()

        print "start is %s" % our_geo.reverse_geocode(start.get_lat(), start.get_lon())
        print "end is %s" % our_geo.reverse_geocode(end.get_lat(), end.get_lon())



        our_otp = otp.OTP(start, end, mode, write_day(curr_month, curr_day, curr_year), write_time(curr_hour, curr_minute), self.has_bike)
        lst_of_trips = our_otp.get_all_trips(0,0,0)

        print "len(lst_of_trips) = %s" % len(lst_of_trips)
        scores = [ ]

        for trip in lst_of_trips:
            scores.append(self.get_score_for_trip(trip))

        top = self.get_top(lst_of_trips, scores)

        return top

    def get_score_for_trip(self, trip):
        noises = parse_noise()
        beauties = parse_beauty()

        noises = normalize_scores(noises)
        beauties = normalize_scores(beauties)
        noise_score, beauty_score, crowd_score = 0, 0, 0
        
        # for crowd in crowds:
        #     crowd.add_time(trip.end_time)

        print "length of trip.sections is %s" % len(trip.sections)

        for section in trip.sections:
            print "length of section.points is %s" % len(section.points)
            for point in section.points:
                print 
                print point
                noise_score += get_noise_score(point.get_lat(), point.get_lon(), noises)
                beauty_score += get_beauty_score(point.get_lat(), point.get_lon(), beauties)



        for crowd in self.user_base.crowd_areas.itervalues():
            crowd.update_times(trip.start_time)
            crowd_score += crowd.get_crowd()



        time = get_time_of_trip(trip)
        top_score = self.utilities['noise']*noise_score + self.utilities['scenery']*beauty_score - self.utilities['crowded']*crowd_score
        return top_score


    def get_top(self, lst_of_trips, scores):
        arg_max = 9999999999
        max_score = 0
        i = 0
        while i < len(scores):
            if scores[i] > max_score:
                arg_max = i
                max_score = scores[i]
            i += 1
        return lst_of_trips[arg_max]

    def increment_utility(self, which):
        self.utilities[which] += 1

    def increase_utility_by_n(self, which, n):
        self.utilities[which] += n

    def normalize_utilities(self):
        self.utilities.normalize()



def get_time_of_trip(trip):
    return trip.end_time - trip.start_time


class Area:

    def __init__(self, name, tl, br, beauty=None, noise=None):
        self.name = name
        self.bounding_box = (tl, br)
        self.beauty = beauty
        self.noise = noise
        self.times = set()

    def point_in_area(self, lat, lng):
        return in_bounding_box(lat, lng, self.bounding_box)

    def add_time(self, time):
        self.times.add(time)

    def get_crowd(self):
        return len(self.times)

    def update_times(self, time_by):
        for time in self.times:
            if time < time_by:
                self.times.remove(time)

    def update_to_now(self):
        self.update_times(datetime.datetime.now())


def in_bounding_box(lat, lon, bounding_box):
    return bounding_box[1][0] <= lat and lat <= bounding_box[0][0] and bounding_box[0][1] <= lon and lon <= bounding_box[1][1]
    
def parse_noise():
    noise_file = open("emission/ce186/NoiseLatLong.csv")
    noise_areas = [ ]
    for noise_line in noise_file:
        noise_line = noise_line.split(',')
        name = noise_line[0]
        tl = (float(noise_line[1]), float(noise_line[2]))
        br = (float(noise_line[3]), float(noise_line[4]))
        noise = int(noise_line[5])
        a = Area(name, tl, br, noise=noise)
        noise_areas.append(a)
    return noise_areas

def parse_beauty():
    beauty_file = open("emission/ce186/beauty.csv")
    beauty_areas = [ ]
    for beauty_line in beauty_file:
        beauty_line = beauty_line.split(',')
        name = beauty_line[0]
        tl = (float(beauty_line[1]), float(beauty_line[2]))
        br = (float(beauty_line[5]), float(beauty_line[6]))
        beauty = int(beauty_line[9])
        a = Area(name, tl, br, beauty=beauty)
        beauty_areas.append(a)
    return beauty_areas



def get_noise_score(lat, lng, noises):
    tot = 0
    for noise_area in noises:
        tot += noise_area.noise
        if noise_area.point_in_area(lat, lng):
            return noise_area.noise
    return float(tot) / float(len(noises)) ## if point isnt in any mapped area return the average

def normalize_scores(areas):
    counter = emmc.Counter()
    print "len of areas is %s" % len(areas)
    for area in areas:
        if area.beauty:
            counter[area.name] = area.beauty
        elif area.noise:
            counter[area.name] = area.noise
    counter.normalize()
    
    new_areas = [ ]
    for name, value in counter.iteritems():
        for area in areas:
            if area.name == name:
                if area.beauty:
                    new_area = Area(name, area.bounding_box[0], area.bounding_box[1], beauty=value)
                elif area.noise:
                    new_area = Area(name, area.bounding_box[0], area.bounding_box[1], noise=value)
                new_areas.append(new_area)

    return new_areas
 
def get_beauty_score(lat, lng, beauties):
    tot = 0
    for beauty_area in beauties:
        tot += beauty_area.beauty
        if beauty_area.point_in_area(lat, lng):
            return beauty_area.beauty
    return float(tot) / float(len(beauties)) ## if point isnt in any mapped area return the average


def test():
    base = UserBase()
    josh = UserModel("josh", base)
    josh.increase_utility_by_n("scenery", 20)
    josh.increase_utility_by_n("noise", 10)
    top_choice = josh.get_top_choice_lat_lng(to.Coordinate(37.8691323,-122.2549288), to.Coordinate(37.8755814,-122.2589025))
    print_path(top_choice)

def find_route(user_base, user, start, end):
    our_user = user_base.get_user(user)
    our_user.get_top_choice_lat_lng(start, end)

def print_path(trip):
    print len(trip.sections)
    for section in trip.sections:
        for point in section.points:
            print point.get_lat(), point.get_lon()
    print trip.trip_end_location.get_lat(), trip.trip_end_location.get_lon()





def write_day(month, day, year):
    return "%s-%s-%s" % (month, day, year)

def write_time(hour, minute):
    return "%s:%s" % (hour, minute)



PLACES = {"cafe_strada" : to.Coordinate(37.8691582,-122.2569807), "jacobs_hall" : to.Coordinate(37.8755764,-122.2584384), 
            "li_ka_shing" : to.Coordinate(37.872931, -122.265220), "i_house" : to.Coordinate(37.869794, -122.252015)}

def make_random_user(base):
    name = str(random.random())
    user = UserModel(name, base)
    utilites = ("sweat", "scenery", "social", "time", "noise", "crowded")
    for u in utilites:
        new_utility = random.randint(1, 101)
        user.increase_utility_by_n(u, new_utility)
    return user

def make_user_base(size):
    user_base = UserBase()
    crowds = parse_starting_pop()
    for _ in xrange(size):
        user = make_random_user(user_base)
        user_base.add_user(user)
    
    for crowd in crowds:
        user_base.add_crowd(crowd)

    return user_base


def parse_starting_pop():
    beauty_file = open("emission/ce186/beauty.csv")
    beauty_areas = [ ]
    for beauty_line in beauty_file:
        beauty_line = beauty_line.split(',')
        name = beauty_line[0]
        tl = (float(beauty_line[1]), float(beauty_line[2]))
        br = (float(beauty_line[5]), float(beauty_line[6]))
        a = Area(name, tl, br)
        beauty_areas.append(a)
    return beauty_areas

def run_simulation():
    print "creating users"
    user_base = make_user_base(100)

    print "putting 20 users on their way at 8am"
    time_now = datetime.datetime(2015, 11, 18, 8, 0, 0)
    user_num = 0
    for user in user_base.users.itervalues():
        if user_num > 19:
            break
        try:
            user.get_top_choice_lat_lng(random.choice(PLACES.values()), random.choice(PLACES.values()), time_now)
        except:
            "error skipping"
        user_num += 1

    print "two minutes later, lets see how this effects routing"
    time_now = datetime.datetime(2015, 11, 18, 8, 2, 0)
    for user in user_base.users.itervalues():
        if user_num > 49:
            break
        try:
            user.get_top_choice_lat_lng(random.choice(PLACES.values()), random.choice(PLACES.values()), time_now)
        except:
            "error skipping"        
        user_num += 1

        
