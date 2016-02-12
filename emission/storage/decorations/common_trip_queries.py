import logging
import pymongo
import pickle
import numpy as np
import bson

import emission.core.wrapper.common_trip as ecwct
import emission.core.get_database as edb


# constants
DAYS_IN_WEEK = 7
HOURS_IN_DAY = 24

#################################################################################
############################ database functions #################################
#################################################################################


def save_common_trip(common_trip):
    db = edb.get_common_trip_db()
    probs = _2d_array_to_mongo_format(common_trip.probabilites)
    db.insert({
        "user_id" : common_trip.user_id,
        "start_loc" : common_trip.start_loc,
        "end_loc" : common_trip.end_loc,
        "trips" : common_trip["trips"],
        "probabilites" : probs
        }) 

def get_common_trip_from_db(user_id, start_place_id, end_place_id):
    db = edb.get_common_trip_db()
    json_obj = db.find_one({"user_id" : user_id, "start_loc" : start_place_id, "end_loc" : end_place_id})
    return make_common_trip_from_json(json_obj)

def get_all_common_trips_for_user(user_id):
    db = edb.get_common_trip_db()
    return db.find({"user_id" : user_id})

def make_common_trip_from_json(json_obj):
    probs = _mongo_to_2d_array(json_obj["probabilites"])
    props = {
        "user_id" : json_obj["user_id"],
        "start_loc" : json_obj["start_loc"],
        "end_loc" : json_obj["end_loc"],
        "trips" : json_obj["trips"],
        "probabilites" : probs
    }
    return ecwct.CommonTrip(props)


def _2d_array_to_mongo_format(array):
    return bson.binary.Binary(pickle.dumps(array, protocol=2), subtype=128)

def _mongo_to_2d_array(mongo_thing):
    return pickle.loads(mongo_thing)

def make_new_common_trip(props=None):
    if props:
        return ecwct.CommonTrip(props)
    return ecwct.CommonTrip()


##############################################################################

def get_weight(common_trip):
    return len(common_trip["trips"])

def add_real_trip_id(trip, _id):
    trip.trips.append(_id)

def get_start_hour(section_info):
    return section_info.start_time.hour

def get_day(section_info):
    return section_info.start_time.weekday()

def increment_probability(trip, day, hour):
    trip.probabilites[day, hour] += 1

def set_up_trips(list_of_cluster_data, user_id):
    assert len(list_of_cluster_data) > 0
    for dct in list_of_cluster_data:
        print "inside of here"
        start_coords = dct['start_coords']
        end_coords = dct['end_coords']
        start_place_id = "%s%s" % (user_id, start_coords)
        end_place_id = "%s%s" % (user_id, end_coords)
        #print 'dct["sections"].trip_id %s is' % dct["sections"][0]
        probabilites = np.zeros((DAYS_IN_WEEK, HOURS_IN_DAY))
        trips = [sec.trip_id for sec in dct["sections"]]
        for sec in dct["sections"]:
            probabilites[get_day(sec), get_start_hour(sec)] += 1
        trip = make_new_common_trip()
        trip.user_id = user_id
        trip.start_loc = start_place_id
        trip.end_loc = end_place_id
        trip.probabilites = probabilites
        trip.trips = trips
        save_common_trip(trip)