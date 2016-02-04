import logging
import pymongo
import pickle
import numpy as np

import emisssion.core.wrapper.common_trip as ecwct
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
        "trips" : common_trip.trips,
        "probs" : probs
        }) 

def get_common_trip_from_db(_id):
    db = edb.get_common_trip_db()
    json_obj = db.find_one({"_id" : _id})
    return make_common_trip_from_json(json_obj)

def make_common_trip_from_json(json_obj):
    probs = _mongo_to_2d_array(json_obj.get(probs))
    props = {
        "user_id" : json_obj.get("user_id"),
        "_id" : json_obj.get("_id"),
        "start_loc" : json_obj.get("start_loc"),
        "end_loc" : json_obj.get("end_loc"),
        "trips" : json_obj.get("trips"),
        "probabilites" : probs
    }
    return ecwct.CommonTrip(props)

def _2d_array_to_mongo_format(array):
    return pymongo.binary.Binary(pickle.dumps(array, protocol=2))

def _mongo_to_2d_array(mongo_thing):
    return pickle.loads(mongo_thing)

def make_common_trip(props):
    return ecwct.CommonTrip(props)

##############################################################################

def get_weight(common_trip):
    return len(common_trip.trips)

def get_astcf_distance(common_trip):
    """ returns the as the crow flies distance of the trip """
    return -1

def make__id(user_id, start_loc_id, end_loc_id):
    return "%s%s%s" % (user_id, start_loc_id, end_loc_id)

def add_real_trip_id(trip, _id):
    trip.trips.append(_id)

def get_start_hour(section_info):
    return section_info.start_time.day

def get_day(section_info):
    return section_info.start_time.weekday()

def increment_probability(trip, day, hour):
    trip.probabilites[day, hour] += 1

def set_up_trips(list_of_cluster_data, user_id):
    for dct in list_of_cluster_data:
        start_coords = dct['start_coords']
        end_coords = dct['end_coords']
        start_place_id = "%s%s" % (user_id, start_coords)
        end_place_id = "%s%s" % (user_id, end_coords)
        trip_props = {
            "user_id" : user_id,
            "_id" : make__id(user_id, start_place_id, end_place_id),
            "start_loc" : start_place_id, 
            "end_loc" : end_place_id,
            "trips" : [],
            "probabilites" : np.zeros((DAYS_IN_WEEK, HOURS_IN_DAY))
        }
        trip = make_common_trip(trip_props)
        for sec in dct["sections"]:
            add_real_trip_id(trip, sec.trip_id)
        save_common_trip(trip)

def add_probabilites(list_of_cluster_data, user_id):
    for dct in list_of_cluster_data:
        start_coords = dct['start_coords']
        end_coords = dct['end_coords']
        start_place_id = "%s%s" % (user_id, start_coords)
        end_place_id = "%s%s" % (user_id, end_coords)
        _id = make__id(user_id, start_place_id, end_place_id)
        trip = get_common_trip_from_db(_id)
        for sec in dct["sections"]:
            increment_probability(trip, get_day(sec), get_start_hour(sec))       