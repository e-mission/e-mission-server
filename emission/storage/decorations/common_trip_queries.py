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
        "trips" : common_trip.trips,
        "probs" : probs,
        "_id" : common_trip.common_trip_id
        }) 

def get_common_trip_from_db(_id):
    db = edb.get_common_trip_db()
    json_obj = db.find_one({"_id" : _id})
    print json_obj
    return make_common_trip_from_json(json_obj)

def get_all_common_trips_for_user(user_id):
    db = edb.get_common_trip_db()
    return db.find({"user_id" : user_id})

def make_common_trip_from_json(json_obj):
    print "json_obj = %s" % json_obj
    probs = _mongo_to_2d_array(json_obj.get(probs))
    props = {
        "user_id" : json_obj.get("user_id"),
        "common_trip_id" : json_obj.get("_id"),
        "start_loc" : json_obj.get("start_loc"),
        "end_loc" : json_obj.get("end_loc"),
        "trips" : json_obj.get("trips"),
        "probabilites" : probs
    }
    return ecwct.CommonTrip(props)

def _2d_array_to_mongo_format(array):
    return bson.binary.Binary(pickle.dumps(array, protocol=2))

def _mongo_to_2d_array(mongo_thing):
    return pickle.loads(mongo_thing)

def make_common_trip(props):
    return ecwct.CommonTrip(props)

def make_new_common_trip(user_id, start, end):
    props = {
        "user_id" : user_id,
        "common_trip_id" : "%s%s%s" % (user_id, start, end),
        "start_loc" : start.common_place_id,
        "end_loc" : end.common_place_id,
        "trips" : (), 
        "probabilites" : np.zeros((DAYS_IN_WEEK, HOURS_IN_DAY))
    }
    return make_common_trip(props)


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
        trips = [sec.trip_id for sec in dct["sections"]]
        probabilites = np.zeros((DAYS_IN_WEEK, HOURS_IN_DAY))
        for sec in dct["sections"]:
            probabilites[get_day(sec), get_start_hour(sec)] += 1
        trip_props = {
            "user_id" : user_id,
            "common_trip_id" : make__id(user_id, start_place_id, end_place_id),
            "start_loc" : start_place_id, 
            "end_loc" : end_place_id,
            "trips" : trips,
            "probabilites" : probabilites
        }
        trip = make_common_trip(trip_props)
        save_common_trip(trip)
