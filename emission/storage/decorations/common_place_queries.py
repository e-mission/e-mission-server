import emission.core.wrapper.common_place as ecwcp
import emission.core.get_database as edb

#################################################################################
############################ database functions #################################
#################################################################################

def save_common_place(common_place):
    db = edb.get_common_place_db()
    db.save(common_place)

def get_common_place_from_db(_id):
    db = edb.get_common_place_db()
    json_obj = db.find_one({"_id" : _id})
    return make_common_place_from_json(json_obj)

def get_all_common_places_for_user(user_id):
    db = edb.get_common_place_db()
    return db.find({"user_id" : user_id})

def make_common_place_from_json(json_obj):
    return ecwcp.CommonPlace(json_obj)

def make_new_common_place(user_id, coords):
    props = {
        "user_id" : user_id,
        "coords" : coords,
        "common_place_id" : "%s%s" % (user_id, coords["coordinates"]),
        "edges" : (),
        "successors" : () 
    }
    return ecwcp.CommonPlace(props)

def make_common_place(props):
    return ecwcp.CommonPlace(props)

################################################################################

def add_successor(common_place, new_place):
    common_place.successors.append(new_place)

def add_edge(common_place, edge):
    common_place.edges.append(edge)

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
            places_to_successors[start_place_id] = {"successors" : [], "coords" : start_coords}
        else:
            places_to_successors[start_place_id]["successors"].append(end_place_id)
        if end_place_id not in places_to_successors:
            places_to_successors[end_place_id] = {"successors" : [], "coords" : end_coords}

    for place_id, info in places_to_successors.iteritems():
        props = {
            "user_id" : user_id,
            "coords" : info["coords"],
            "successors" : info["successors"],
            "common_place_id" : place_id
        }
        start = make_common_place(props)
        save_common_place(start)
