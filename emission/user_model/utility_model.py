import emission.simulation.markov_model_counter as emmc
import emission.net.ext_service.otp.otp as otp
import emission.core.our_geocoder as geo
import emission.core.wrapper.trip_old as to
import emission.net.ext_service.gmaps.googlemaps as gmaps
import emission.net.ext_service.gmaps.common as gmcommon
import emission.core.get_database as edb

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

    """ 
    Stores all the users and stores the population of areas  
    Also keeps state on other useful things that we need to know, like caches
    """

    def __init__(self):
        self.users = {}
        self.crowd_areas = {}
        self.last_info = {}  ## so we only call google maps if we change things
        self.old_trips = None
        self.geocode_cache = {} # fewer calls to google maps

    def add_user(self, user):
        self.users[user.name] = user

    def get_user(self, user_name):
        return self.users[user_name]

    def add_crowd(self, area):
        self.crowd_areas[area.name] = area

    def get_crowd_info(self, area_name):
        return self.crowd_areas[area_name]

    def geocode_with_cache(self, place):
        coder = geo.Geocoder()
        if place in self.geocode_cache:
            return self.geocode_cache[place]
        else:
            coded = coder.geocode(place)
            self.geocode_cache[place] = coded
            return coded

class CampusTrip:

    def __init__(self, score_list, time_duration, points, source):
        self.time = score_list[0] 
        self.sweat = score_list[1]
        self.beauty = score_list[2]
        self.social = score_list[3]
        self.tot_score = sum(score_list)
        self.time_duration = time_duration / float(60)
        self.points = points
        self.source = source

    def make_points(self):
        to_return = ""
        for p in self.points:
            to_return += str(p[0])
            to_return += ","
            to_return += str(p[1])
            to_return += ","
        return to_return


    def make_thing_for_isabel(self):
        return '%s;%s;%s;%s;%s;%s' % (self.beauty, self.time, self.social, self.sweat, self.time_duration, self.make_points())

    def make_jsn(self):
        return json.dumps({"time" : self.time, "beauty" : self.beauty, "social" : self.social, "sweat" : self.sweat, "duration" : self.time_duration, "points" : self.points})
        
    def make_json(self):
        return json.dumps(self.make_thing_for_isabel())

    def __repr__(self):
        return "total score : %f || source : %s || beauty : %f || sweat : %f || time : %f || social : %f" % (self.tot_score, self.source, self.beauty, self.sweat, self.time, self.social)

    def __eq__(self, other):
        return self.make_points() == other.make_points()


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
        self.utilities["sweat"] = 0
        self.utilities["scenery"] = 0
        self.utilities["social"] = 0
        self.utilities["time"] = 0

    def get_top_choice_places(self, start_place, end_place):
        our_geo = geo.Geocoder()
        start = our_geo.geocode(start_place)
        end = our_geo.geocode(end_place)
        return self.get_top_choices_lat_lng(start, end)

    def get_all_trips(self, start, end, curr_time=None):
        c = googlemaps.client.Client('AIzaSyAFsQeO3Xj60s0nBVRcAS-I9FLw6KZPV-E')
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

        walk_otp = otp.OTP(start, end, "WALK", write_day(curr_month, curr_day, curr_year), write_time(curr_hour, curr_minute), False)
        lst_of_trips = walk_otp.get_all_trips(0, 0, 0)

        our_gmaps = gmaps.GoogleMaps("AIzaSyAFsQeO3Xj60s0nBVRcAS-I9FLw6KZPV-E") 
        mode = "walking"
        if self.has_bike:
            mode = "bicycling"

        jsn = our_gmaps.directions(start, end, mode)
        gmaps_options = gmcommon.google_maps_to_our_trip(jsn, 0, 0, 0, mode, curr_time)

        ## Throw in a random waypoint to make things more interesting
        waypoint = get_one_random_point_in_radius(CENTER_OF_CAMPUS, RANDOM_RADIUS)
        gmaps_way_points_jsn = our_gmaps.directions(start, end, mode, waypoints=waypoint)
        way_points_options = gmcommon.google_maps_to_our_trip(gmaps_way_points_jsn, 0, 0, 0, mode, curr_time)
        tot_trips = lst_of_trips + gmaps_options + way_points_options

        return tot_trips


    def get_top_choices_lat_lng(self, start, end, curr_time=None):
        tot_trips = self.get_all_trips(start, end, curr_time)
        scores = [ ]
        i = 0
        print "len = %s" % len(tot_trips)
        times = get_normalized_times(tot_trips)
        beauty = get_normalized_beauty(tot_trips)
        sweat = get_normalized_sweat(tot_trips)

        print "sweat = %s" % sweat

        for i in range(len(times)):
            scores.append(self.get_score_for_trip(tot_trips[i], times[i], beauty[i], sweat[i]))

        top = self.get_top_n(scores, N_TOP_TRIPS)
        return top

    def get_score_for_trip(self, trip, time, beauty, sweat):
        crowd_score = 0
        lst_of_points = get_route(trip)

        for crowd in self.user_base.crowd_areas.itervalues():
            crowd.update_times(trip.start_time)
            crowd_score += crowd.get_crowd()

        final_time =  -time * self.utilities["time"]
        final_sweat = -sweat * self.utilities["sweat"]
        final_beauty = (self.utilities['scenery']*beauty)
        final_crowd = (self.utilities['social']*crowd_score)

        final_score_tuple = (final_time, final_sweat, final_beauty, final_crowd)
        print "final_score_tuple : %s" % str(final_score_tuple)
        return CampusTrip(final_score_tuple, get_time_of_trip(trip), lst_of_points, "source")
        

    def get_top_n(self, lst_of_trips, n):
        return heapq.nlargest(n, lst_of_trips, key=lambda v: v.tot_score)

    def increment_utility(self, which):
        self.utilities[which] += 1

    def increase_utility_by_n(self, which, n):
        self.utilities[which] += n

    def normalize_utilities(self):
        self.utilities.normalize()

    def save_to_db(self):
        db = edb.get_utility_model_db()
        db.insert({"utilities" : self.utilities, "name" : self.name})

    def delta(self, start, end):
        """
        Returns true if anything has changed and we should call google maps...
        Otherwise no.
        """
        if "start" not in self.user_base.last_info or "end" not in self.user_base.last_info or "utilities" not in self.user_base.last_info:
            #print "first delta"
            return True
        return not (start == self.user_base.last_info["start"] and end == self.user_base.last_info["end"] and self.utilities == self.user_base.last_info["utilities"])

    def add_to_last(self, start, end):
        self.user_base.last_info["utilities"] = self.utilities.copy()
        self.user_base.last_info["start"] = start
        self.user_base.last_info["end"] = end


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
        i += 1
    counter.normalize()
    to_return = []
    for i in range(len(lst_of_trips)):
        to_return.append(counter[i])
    return to_return

def get_sweat_factor(trip):
    chng = get_elevation_change(trip)
    print "chng : %s" % str(chng)
    return 71.112*chng[0] + 148.09

def get_normalized_sweat(lst_of_trips):
    counter = emmc.Counter()
    i = 0
    for trip in lst_of_trips:
        factor = get_sweat_factor(trip)
        print "sweat_factor : %s" % factor
        counter[i] = factor
        i += 1
    counter.normalize()
    to_return = []
    for i in range(len(lst_of_trips)):
        to_return.append(counter[i])
    return to_return

def get_normalized_beauty(lst_of_trips):
    counter = emmc.Counter()
    i = 0
    for trip in lst_of_trips:
        factor = get_beauty_score_of_trip(trip)
        print "beauty_factor : %s" % factor
        counter[i] = factor
        i += 1
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

    def __repr__(self):
        return "beauty : %s" % (self.beauty)


def in_bounding_box(lat, lon, bounding_box):
    return bounding_box[1][0] <= lat and lat <= bounding_box[0][0] and bounding_box[0][1] <= lon and lon <= bounding_box[1][1]
    
def parse_noise():
    noise_file = open("emission/user_model/noise_data.csv")
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
    beauty_file = open("emission/user_model/beauty.csv")
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
            to_return += get_closest(time, noise_area)
    if to_return > 0:
        return to_return
    return .5 ## if point isnt in any mapped area return the average

def get_closest(time, area):
    for k, v in area.time_to_noise.iteritems():
        if time - k < datetime.timedelta(minutes=10):
            return v
    return 0

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


def get_beauty_score_of_trip(trip):
    beauties = parse_beauty()
    beauty_score = 0
    tot_points = 0
    for section in trip.sections:
        for point in section.points:
            tot_points += 1
            beauty_score += get_beauty_score(point.get_lat(), point.get_lon(), beauties)
    return float(beauty_score) / float(tot_points) 


def get_noise_score_of_trip(trip):
    noises = parse_noise()
    noise_score = 0
    for section in trip.sections:
        for point in section.points:
            tot_points += 1
            noise_score += get_noise_score(point.get_lat(), point.get_lon(), noises)
    return float(beauty_score) / float(tot_points) 



def get_route_dict(trip):
    route = []
    for point in trip:
        d = {'lat' : point[0], 'lng' : point[1]}
        route.append(d)
    return route

def get_route(trip):
    lst_of_points = []
    lst_of_points.append( (trip.trip_start_location.get_lat(), trip.trip_start_location.get_lon()) )
    for section in trip.sections:
        for point in section.points:
            lst_of_points.append( (point.get_lat(), point.get_lon()) )
    lst_of_points.append( (trip.trip_end_location.get_lat(), trip.trip_end_location.get_lon()) )
    return lst_of_points

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
    at = int((datetime.datetime.utcnow() - datetime.datetime.utcfromtimestamp(0)).total_seconds())
    jsn = json.load(response)

    print jsn
    info = make_user_from_jsn(jsn, base)
    user = info["user"]
    #print info["time_info"]
    if user.delta(info["start"], info["end"]):
        #print "IN DELTA=TRUE"
        trips = user.get_top_choices_lat_lng(info["start"], info["end"], info["time_info"]["when"])
        user.user_base.old_trips = trips
        user.add_to_last(info["start"], info["end"])
    else:
        trips = user.user_base.old_trips

    #print "trips is %s" % trips

    query = {}
    header = {'Content-Type':'application/json'}

    ts = ""
    payload = []
    #print "above ts . apppend"
    for t in trips:
        print t.sweat
        #print t
        ts += (str(t.make_thing_for_isabel()))
        ts += ";"

    payload = [{"value" : str(ts), "at" : at}]

    body = json.dumps(payload)
    r = requests.request('post', send_data, data=body, params=query, headers=header, timeout=120)
    #print r.json

def loop_mongo(base):
    info = user_from_mongo()
    if user.delta(info["start"], info["end"]):
        #print "IN DELTA=TRUE"
        trips = user.get_top_choices_lat_lng(info["start"], info["end"], info["time_info"]["when"])
        user.user_base.old_trips = trips
        user.add_to_last(info["start"], info["end"])
    else:
        trips = user.user_base.old_trips

    at = int((datetime.datetime.utcnow() - datetime.datetime.utcfromtimestamp(0)).total_seconds())

    final_json = {"at" : at, "pushing" : True, "trips" : []}
    for t in trips:
        final_json["trips"].append(t.make_jsn())

    edb.get_utility_model_db().insert(json.dumps(final_json))


def main():
    base = UserBase()
    #loop(base)
    while True:
        #print "here"
        #loop(base)
        try:
            loop(base)
            time.sleep(.5)
        except Exception as e:
            print e
            pass



def make_user_from_jsn(jsn, base):
    value_line = jsn["objects"]["Computer"]["streams"]["userData"]["points"][0]["value"]
    value_line = value_line.split(";")
    start = value_line[0]
    end = value_line[1]

    start = base.geocode_with_cache(start)
    end = base.geocode_with_cache(end)
    
    time_info = {}
    print value_line
    if value_line[2] == "leaveNow":
        time_info["leave"] = True
        time_info["when"] = datetime.datetime.now()
        print "leaveNow"
    elif value_line[2] == "leaveAt":
        time_info["leave"] = True
        time_info["when"] = str_time_to_datetme(value_line[3])
        print "leaveAt"
    elif value_line[2] == "thereBy":
        time_info["leave"] = False
        time_info["when"] = str_time_to_datetme(value_line[3])
        print "arriveAt"


    bike = get_bike_info(value_line[4])
    user = UserModel("scottMoura", base, bike)
    user.increase_utility_by_n("time", int(value_line[5]))
    user.increase_utility_by_n("sweat", int(value_line[6]))
    user.increase_utility_by_n("scenery", int(value_line[7]))
    user.increase_utility_by_n("social", int(value_line[8]))

    user.utilities.normalize()
    print "utilities : %s" % user.utilities

    return {"user" : user, "start" : start, "end" : end, "time_info" : time_info}

def user_from_mongo(base):
    db = edb.get_utility_model_db()
    record = db.find({"pushing" : False}).sort({"at" : -1}).limit(1)
    bike = record["bike"]

    user = UserModel("person", base, bike)
    user.increase_utility_by_n("time", record["time"])
    user.increase_utility_by_n("sweat", record["sweat"])
    user.increase_utility_by_n("scenery", record["scenery"])
    user.increase_utility_by_n("social", record["social"])

    return {"user" : user, "start" : record["start"], "end" : record["end"], "time_info" : record["time_info"]}   


def get_bike_info(bike_str):
    if bike_str == "walk":
        return False
    return True

def get_elevation_change(trip):
    c = googlemaps.client.Client('AIzaSyAFsQeO3Xj60s0nBVRcAS-I9FLw6KZPV-E')
    jsn = googlemaps.elevation.elevation_along_path(c, get_route(trip), 50)
    up, down = 0, 0
    prev = None
    for item in jsn:
        if item["location"]["lat"] == 0:
            return (0, 0)
        if prev and item["elevation"] > prev:
            up += item["elevation"] - prev
        elif prev and item["elevation"] < prev:
            down += prev - item["elevation"]
        prev = item['elevation']
    return (up, down)


if __name__ == "__main__":
    main()

#[{"score (for each)": 0, "trip_duration" : 12, "points" : points list]