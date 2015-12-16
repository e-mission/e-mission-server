import emission.simulation.markov_model_counter as emmc
import emission.net.ext_service.otp.otp as otp
import emission.core.our_geocoder as geo
import emission.core.wrapper.trip_old as to
import emission.net.ext_service.gmaps.googlemaps as gmaps
import emission.net.ext_service.gmaps.common as gmcommon

import datetime
import random
import math
import urllib2
import json
import heapq
import time
import googlemaps
import requests

CENTER_OF_CAMPUS = to.Coordinate(37.871790, -122.260005)
RANDOM_RADIUS = .3  # 300 meters around center of campus; for randomization
N_TOP_TRIPS = 3 # Number of top trips we return for the user to look at

class UserBase:

    """ Stores all the users and stores the population of areas """

    PLACES = {"cafe_strada" : to.Coordinate(37.8691582,-122.2569807), "jacobs_hall" : to.Coordinate(37.8755764,-122.2584384), 
        "li_ka_shing" : to.Coordinate(37.872931, -122.265220), "i_house" : to.Coordinate(37.869794, -122.252015)}

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

class CampusTrip:

    def __init__(self, score_list, time_duration, points):
        self.time = score_list[0]
        self.sweat = score_list[1]
        self.beauty = score_list[2]
        self.social = score_list[3]
        self.tot_score = sum(score_list)
        self.time_duration = time_duration
        self.points = points

    def make_thing_for_isabel(self):
        points_list = get_route_dict(self.points)
        return {"beauty" : self.beauty, "time" : self.time, "social" : self.social, "sweat" : self.sweat, "time_duration" : self.time_duration, "points" : self.points}
        
    def make_json(self):
        return json.dumps(self.make_thing_for_isabel())


class UserModel:

    """ 
    User Model class  
    Can do lots of cool things
    """ 

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

        # print "start is %s" % our_geo.reverse_geocode(start.get_lat(), start.get_lon())
        # print "end is %s" % our_geo.reverse_geocode(end.get_lat(), end.get_lon())

        our_otp = otp.OTP(start, end, mode, write_day(curr_month, curr_day, curr_year), write_time(curr_hour, curr_minute), self.has_bike)
        lst_of_trips = our_otp.get_all_trips(0,0,0)

        our_gmaps = gmaps.GoogleMaps("AIzaSyBEkw4PXVv_bsAdUmrFwatEyS6xLw3Bd9c") 
        mode = "walking"
        if self.has_bike:
            mode = "bicycling"

        jsn = our_gmaps.directions(start, end, mode)
        gmaps_options = gmcommon.google_maps_to_our_trip(jsn, 0, 0, 0, mode, curr_time)

        ## Throw in a random waypoint to make things more interesting
        waypoint = get_one_random_point_in_radius(CENTER_OF_CAMPUS, RANDOM_RADIUS)
        gmaps_way_points_jsn = our_gmaps.directions(start, end , mode, waypoints=waypoint)
        way_points_options = gmcommon.google_maps_to_our_trip(gmaps_way_points_jsn, 0, 0, 0, mode, curr_time)
        

        times = get_normalized_times(lst_of_trips)
        times.extend(get_normalized_times(gmaps_options))
        times.extend(get_normalized_times(way_points_options))

        sweat = get_normalized_sweat(lst_of_trips)
        sweat.extend(get_normalized_sweat(gmaps_options))
        sweat.extend(get_normalized_sweat(way_points_options))
        
        scores = [ ]
        i = 0
        for trip in lst_of_trips:
            scores.append(self.get_score_for_trip(trip, times[i], sweat[i]))
            i += 1
        for trip in gmaps_options:
            scores.append(self.get_score_for_trip(trip, times[i], sweat[i]))
            i += 1
        for trip in way_points_options:
            scores.append(self.get_score_for_trip(trip, times[i], sweat[i]))
            i += 1

        lst_of_trips.extend(gmaps_options)
        lst_of_trips.extend(way_points_options)
        print "len of lst of trips is %s" % len(lst_of_trips)
        top = self.get_top_n(scores, N_TOP_TRIPS)
        return top

    def get_score_for_trip(self, trip, normalized_time, normalized_sweat):
        noises = parse_noise()
        beauties = parse_beauty()

        noises = normalize_noises(noises)
        beauties = normalize_scores(beauties)
        noise_score, beauty_score, crowd_score = 0, 0, 0

        lst_of_points = []
        
        tot_points = 0
        for section in trip.sections:
            for point in section.points:
                tot_points += 1
                noise_score += get_noise_score(point.get_lat(), point.get_lon(), noises, trip.start_time)
                beauty_score += get_beauty_score(point.get_lat(), point.get_lon(), beauties)
                lst_of_points.append( (point.get_lat(), point.get_lon()) )

        for crowd in self.user_base.crowd_areas.itervalues():
            crowd.update_times(trip.start_time)
            crowd_score += crowd.get_crowd()

        final_time = (self.utilities["time"]*normalized_time) / tot_points
        final_sweat = (self.utilities["sweat"]*normalized_sweat) / tot_points
        final_beauty = (self.utilities['scenery']*beauty_score) / tot_points
        final_crowd = (self.utilities['social'] * ((noise_score + crowd_score) / 2.0)) / tot_points

        final_score_tuple = (final_time, final_sweat, final_beauty, final_crowd)
        return CampusTrip(final_score_tuple, get_time_of_trip(trip), lst_of_points)
        

    def get_top_n(self, lst_of_trips, n):
        return heapq.nlargest(n, lst_of_trips, key=lambda v: v.tot_score)

    def increment_utility(self, which):
        self.utilities[which] += 1

    def increase_utility_by_n(self, which, n):
        self.utilities[which] += n

    def normalize_utilities(self):
        self.utilities.normalize()


def normalize_noises(noise_areas):
    to_return = []
    for area in noise_areas:
        area.normalize_sounds()
        to_return.append(area)
    return to_return

def get_time_of_trip(trip):
    return (trip.end_time - trip.start_time).seconds

def get_normalized_times(lst_of_trips):
    counter = emmc.Counter()
    i = 0
    for trip in lst_of_trips:
        counter[i] = get_time_of_trip(trip)
    counter.normalize()
    to_return = []
    for i in range(len(lst_of_trips)):
        to_return.append(counter[i])
    return to_return

def get_sweat_factor(trip):
    start = trip.trip_start_location.to_tuple()
    end = trip.trip_end_location.to_tuple()
    chng = get_elevation_change(start, end)
    if chng[0] < chng[1]:
        return 0
    return 71.112*chng[0] + 148.09

def get_normalized_sweat(lst_of_trips):
    counter = emmc.Counter()
    i = 0
    for trip in lst_of_trips:
        counter[i] = get_sweat_factor(trip)
    counter.normalize()
    to_return = []
    for i in range(len(lst_of_trips)):
        to_return.append(counter[i])
    return to_return

class Area:

    """ Area class """ 

    def __init__(self, name, tl, br, beauty=None, time_to_noise=None):
        self.name = name
        self.bounding_box = (tl, br)
        self.beauty = beauty
        self.time_to_noise = time_to_noise
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

    def normalize_sounds(self):
        counter = emmc.Counter()
        for k,v in self.time_to_noise.iteritems():
            counter[k] = v
        counter.normalize()
        self.time_to_noise = counter


def in_bounding_box(lat, lon, bounding_box):
    return bounding_box[1][0] <= lat and lat <= bounding_box[0][0] and bounding_box[0][1] <= lon and lon <= bounding_box[1][1]
    
def parse_noise():
    noise_file = open("emission/ce186/noise_data.csv")
    sproul_noises, glade_noises, wellmen_noises, leconte_noises = {}, {}, {}, {}
    sproul_tl, sproul_br = (37.870637,-122.259722), (37.868926,-122.259005)
    glade_tl, glade_br = (37.87359,-122.260098), (37.872707,-122.258687)
    wellmen_tl, wellmen_br = (37.873045,-122.263377), (37.872501,-122.261803)
    lecont_tl, lecont_br = (37.873278,-122.256959), (37.872277,-122.25639)
    time = datetime.datetime(2040, 10, 10, 6, 0, 0)
    td = datetime.timedelta(minutes=10)
    for l in noise_file:
        l = l.split(',')
        sproul_noises[time] = float(l[0])
        glade_noises[time] = float(l[1])
        wellmen_noises[time] = float(l[2])
        leconte_noises[time] = float(l[3])
    sproul = Area("sproul", sproul_tl, sproul_br, time_to_noise=sproul_noises)
    glade = Area("glade", glade_tl, glade_br, time_to_noise=glade_noises)
    wellmen = Area("wellmen", wellmen_tl, wellmen_br, time_to_noise=wellmen_noises)
    leconte = Area("leconte", lecont_tl, lecont_br, time_to_noise=leconte_noises)
    return [sproul, glade, wellmen, leconte]
    

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

def get_noise_score(lat, lng, noises, time):
    tot = 0
    to_return = 0
    for noise_area in noises:
        if noise_area.point_in_area(lat, lng):
            to_return += get_closest(time, area)
    if to_return > 0:
        return to_return
    return .5 ## if point isnt in any mapped area return the average

def get_closest(time, area):
    for k, v in area.time_to_noise.iteritems():
        if time - k < datetime.timedelta(minutes=10):
            return v

def normalize_scores(areas):
    counter = emmc.Counter()
    #print "len of areas is %s" % len(areas)
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
    josh.increase_utility_by_n("scenery", 100)
    josh.increase_utility_by_n("noise", 10)
    top_choice = josh.get_top_choice_lat_lng(to.Coordinate(37.8691323,-122.2549288), to.Coordinate(37.875509, -122.257048))
    #print_path(top_choice)

def find_route(user_base, user, start, end):
    our_user = user_base.get_user(user)
    our_user.get_top_choice_lat_lng(start, end)

def get_route_dict(trip):
    route = []
    for point in trip:
        d = {"lat" : point[0], "lng" : point[1]}
        route.append(d)
    return route

def get_route(trip):
    #print len(trip.sections)
    route = []
    for section in trip.sections:
        for point in section.points:
            route.append( (point.get_lat(), point.get_lon()) )
    route.append( (trip.trip_end_location.get_lat(), trip.trip_end_location.get_lon()) )
    return route

def write_day(month, day, year):
    return "%s-%s-%s" % (month, day, year)

def write_time(hour, minute):
    return "%s:%s" % (hour, minute)

def make_random_user(base):
    name = str(random.random())
    user = UserModel(name, base)
    utilites = ("sweat", "scenery", "social", "time", "noise", "crowded")
    for u in utilites:
        new_utility = random.randint(1, 101)
        user.increase_utility_by_n(u, new_utility)
    return user


def get_one_random_point_in_radius(crd, radius):
    # From https://gis.stackexchange.com/questions/25877/how-to-generate-random-locations-nearby-my-location
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
    to_return = to.Coordinate(y + y_0, x + x_0)
    return to_return

def kilometers_to_degrees(km):
    ## From stackexchnage mentioned above 
    return (float(km)/float(40000)) * 360


def str_time_to_datetme(str_time):
    t = str_time.split(":")
    return datetime.datetime(2040, 10, 10, int(t[0]), int(t[1]), 0)

def loop(base):
    get_data = 'http://127.0.0.1:5000/network/Project/object/Computer/stream/userData'
    send_data = 'http://127.0.0.1:5000/network/Project/object/Computer/stream/processReturn'
    request = urllib2.Request(get_data)
    response = urllib2.urlopen(request)
    jsn = json.load(response)
    info = make_user_from_jsn(jsn, base)
    user = info["user"]
    trips = user.get_top_choice_lat_lng(info["start"], info["end"], info["time_info"]["when"])
    query = {}
    header = {'Content-Type':'application/json'}
    payload = []
    for t in trips:
        payload.append(t.make_thing_for_isabel())
    # Set body (also referred to as data or payload). Body is a JSON string.
    body = json.dumps(payload)
    # Form and send request. Set timeout to 2 minutes. Receive response.
    r = requests.request('post', send_data, data=body, params=query, headers=header, timeout=120 )
    print r.json

def main():
    base = UserBase()
    while True:
        try:
            loop(base)
            time.sleep(1)
        except:
            print "failed"
            pass



def make_user_from_jsn(jsn, base):
    value_line = jsn["objects"]["Computer"]["streams"]["userData"]["points"][0]["value"]
    value_line = value_line.split(";")
    coder = geo.Geocoder()

    
    start = value_line[0]
    end = value_line[1]

    start = coder.geocode(start)
    end = coder.geocode(end)
    
    time_info = {}
    if value_line[2] == "leaveNow":
        time_info["leave"] = True
        time_info["when"] = datetime.datetime.now()
    elif value_line[2] == "leaveAt":
        time_info["leave"] = True
        time_info["when"] = str_time_to_datetme(value_line[3])
    elif value_line[2] == "arriveAt":
        time_info["leave"] = False
        time_info["when"] = str_time_to_datetme(value_line[3])

    bike = get_bike_info(value_line[4])
    user = UserModel("scottMoura", base, bike)
    user.increase_utility_by_n("time", int(value_line[5]))
    user.increase_utility_by_n("sweat", int(value_line[6]))
    user.increase_utility_by_n("beauty", int(value_line[7]))
    user.increase_utility_by_n("crowded", int(value_line[8]))
    return {"user" : user, "start" : start, "end" : end, "time_info" : time_info}


def get_bike_info(bike_str):
    return False

def get_elevation_change(pnt1, pnt2):
    c = googlemaps.client.Client('AIzaSyBEkw4PXVv_bsAdUmrFwatEyS6xLw3Bd9c')
    jsn = googlemaps.elevation.elevation_along_path(c, (pnt1, pnt2), 10)
    up, down = 0, 0
    prev = None
    for item in jsn:
        if prev and item["elevation"] > prev:
            up += item["elevation"] - prev
        elif prev and item["elevation"] < prev:
            down += prev - item["elevation"]
        prev = item['elevation']
    return (up, down)



#[{"score (for each)": 0, "trip_duration" : 12, "points" : points list]