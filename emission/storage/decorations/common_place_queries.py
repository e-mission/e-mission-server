import geojson as gj

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

def get_common_place_from_db(_id):
    db = edb.get_common_place_db()
    json_obj = db.find_one({"common_place_id" : _id})
    return make_common_place(json_obj)

def get_all_common_places_for_user(user_id):
    db = edb.get_common_place_db()
    return db.find({"user_id" : user_id})

def make_new_common_place(user_id, coords, successors):
    place = ecwcp.CommonPlace()
    place.user_id = user_id
    place.coords = coords
    place.common_place_id = "%s%s" % (user_id, coords["coordinates"])
    place.successors = successors
    return place

def make_common_place(props):
    return ecwcp.CommonPlace(props)

################################################################################

def create_places(list_of_cluster_data, user_id):
    places_to_successors = {}
    for dct in list_of_cluster_data:
        start_name = dct['start']
        end_name = dct['end']
        start_coords = dct['start_coords']
        end_coords = dct['end_coords']
        start_place_id = "%s%s" % (user_id, start_coords)
        end_place_id = "%s%s" % (user_id, end_coords)
        if start_place_id not in places_to_successors:
            places_to_successors[start_place_id] = {"successors" : [], "coords" : start_coords.coordinate_list()}
        else:
            places_to_successors[start_place_id]["successors"].append(end_place_id)
        if end_place_id not in places_to_successors:
            places_to_successors[end_place_id] = {"successors" : [], "coords" : end_coords.coordinate_list()}

    for place_id, info in places_to_successors.iteritems():
        start = make_new_common_place(user_id, gj.Point(info['coords']), info["successors"])
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
    return esmmc.sampleFromCounter(temp)[0]

def has_succesor(user_id, place_id, time):
    day = time.weekday()
    place = get_common_place_from_db(place_id)
    for suc in place["successors"]:
        trip = esdctp.get_common_trip_from_db(user_id, place_id, suc)
        for temp_hour in xrange(time.hour, esdctp.HOURS_IN_DAY):
            if trip.probabilites[day, temp_hour] > 0:
                return True
    return False