import geojson as gj
import logging

import bson.objectid as boi

import emission.core.wrapper.common_place as ecwcp
import emission.core.get_database as edb
import emission.simulation.markov_model_counter as esmmc
import emission.storage.decorations.common_trip_queries as esdctp

#################################################################################
############################ database functions #################################
#################################################################################

def save_common_place(common_place):
    db = edb.get_common_place_db()
    db.save(common_place)

def get_common_place_from_db(common_place_id):
    db = edb.get_common_place_db()
    json_obj = db.find_one({"_id" : common_place_id})
    return make_common_place(json_obj)

def get_all_common_places_for_user(user_id):
    db = edb.get_common_place_db()
    return db.find({"user_id" : user_id})

def get_common_place_at_location(loc):
    db = edb.get_common_place_db()
    return make_common_place(db.find_one({"location": loc}))

def make_new_common_place(user_id, loc):
    place = ecwcp.CommonPlace()
    place.user_id = user_id
    place.location = loc
    return place

def make_common_place(props):
    return ecwcp.CommonPlace(props)

def clear_existing_places(user_id):
    db = edb.get_common_place_db()
    db.remove({'user_id': user_id})

def get_all_place_objs(common_place):
    trip.trips = [unc_trip.get_id() for unc_trip in dct["sections"]]
    place_db = edb.get_place_db()
    start_places = []
    end_places = []
    for t in trip.trips:
        start = place_db.find_one({"_id" : t.start_place})
        end = place_db.find_one({"_id" : t.end_place})
        start_places.append(start)
        end_places.append(end)

################################################################################

def create_places(list_of_cluster_data, user_id):
    places_to_successors = {}
    places_dct = {}
    logging.debug("About to create places for %d clusters" % len(list_of_cluster_data))
    for dct in list_of_cluster_data:
        start_name = dct['start']
        end_name = dct['end']
        start_loc = gj.Point(dct['start_coords'].coordinate_list())
        end_loc = gj.Point(dct['end_coords'].coordinate_list())
        start_loc_str = gj.dumps(start_loc, sort_keys=True)
        end_loc_str = gj.dumps(end_loc, sort_keys=True)
        if start_loc_str not in places_to_successors:
            places_to_successors[start_loc_str] = []
        else:
            places_to_successors[start_loc_str].append(end_loc)
        if end_loc_str not in places_to_successors:
            places_to_successors[end_loc_str] = []

        if start_loc_str not in places_dct:
            places_dct[start_loc_str] = dct["start_places"]

        if end_loc_str not in places_dct:
            places_dct[end_loc_str] = dct["end_places"]

    clear_existing_places(user_id)
    logging.debug("After creating map, number of places is %d" % len(places_to_successors))
    for loc_str in places_to_successors.iterkeys():
        start = make_new_common_place(user_id, gj.loads(loc_str))
        logging.debug("Adding %d places for this place" % len(places_dct[loc_str]))
        start.places = places_dct[loc_str]
        save_common_place(start)

    for loc_str, successors in places_to_successors.iteritems():
        start = get_common_place_at_location(gj.loads(loc_str))
        successor_places = map(lambda loc:get_common_place_at_location(loc), successors)
        start.successors = successor_places

        save_common_place(start)

### Graph queries

def get_succesor(user_id, place_id, time):
    temp = esmmc.Counter()
    day = time.weekday()
    place = get_common_place_from_db(place_id)
    for suc in place["successors"]:
        trip = esdctp.get_common_trip_from_db(user_id, place_id, suc)
        for temp_hour in xrange(time.hour, esdctp.HOURS_IN_DAY):
            counter_key = ("%s" % suc, temp_hour)
            temp[counter_key] = trip.probabilites[day, temp_hour]
    return boi.ObjectId(esmmc.sampleFromCounter(temp)[0])

def has_succesor(user_id, place_id, time):
    day = time.weekday()
    place = get_common_place_from_db(place_id)
    for suc in place["successors"]:
        trip = esdctp.get_common_trip_from_db(user_id, place_id, suc)
        for temp_hour in xrange(time.hour, esdctp.HOURS_IN_DAY):
            if trip.probabilites[day, temp_hour] > 0:
                return True
    return False
