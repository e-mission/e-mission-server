import emission.ce186.utility_model as ecum
import emission.core.wrapper.trip_old as trip

import random
import datetime


PLACES = {"cafe_strada" : trip.Coordinate(37.8691582,-122.2569807), "jacobs_hall" : trip.Coordinate(37.8755764,-122.2584384), 
            "li_ka_shing" : trip.Coordinate(37.872931, -122.265220), "i_house" : trip.Coordinate(37.869794, -122.252015)}

def make_random_user():
    name = str(random.random())
    user = ecum.UserModel(name)
    utilites = ("sweat", "scenery", "social", "time", "noise", "crowded")
    for u in utilites:
        new_utility = random.randint(1, 101)
        user.increase_utility_by_n(u, new_utility)
    return user

def make_user_base(size):
    user_base = ecum.UserBase()
    crowds = parse_starting_pop()
    for _ in xrange(size):
        user = make_random_user()
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
    user_base = make_user_base(1000)

    print "putting 100 users on their way at 8am"
    time_now = datetime.datetime(2015, 11, 18, 8, 0, 0)
    user_num = 0
    for user in user_base.users.itervalues():
        if user_num > 99:
            break
        user.get_top_choice_lat_lng(random.choice(PLACES.values()), random.choice(PLACES.values), time_now)
        user_num += 1

    print "two minutes later, lets see how this effects routing"
    time_now = datetime.datetime(2015, 11, 18, 8, 2, 0)
    for user in user_base.users.itervalues():
        if user_num > 199:
            break
        user.get_top_choice_lat_lng(random.choice(PLACES.values()), random.choice(PLACES.values), time_now)
        user_num += 1

        
