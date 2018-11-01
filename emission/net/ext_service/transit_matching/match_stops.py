import logging
import json
import requests
import attrdict as ad
import itertools
import copy

try:
    config_file = open('conf/net/ext_service/overpass_server.json')
except:
    print("overpass not configured, falling back to default overleaf.de")
    config_file = open('conf/net/ext_service/overpass_server.json.sample')

try:
    query_file = open('conf/net/ext_service/overpass_transit_stops_query_template')
except:
    print("transit stops query not configured, falling back to default")
    query_file = open('conf/net/ext_service/overpass_transit_stops_query_template.sample')

config_data = json.load(config_file)
url = config_data["url"]

query_string = "".join(query_file.readlines())

def get_public_transit_stops(min_lat, min_lon, max_lat, max_lon):
    bbox_string = "%s,%s,%s,%s" % (min_lat, min_lon, max_lat, max_lon)
    logging.debug("bbox_string = %s" % bbox_string)
    overpass_public_transit_query_template = query_string
    overpass_query = overpass_public_transit_query_template.format(bbox=bbox_string)
    response = requests.post("http://overpass-api.de/api/interpreter", data=overpass_query)
    all_results = response.json()["elements"]
    relations = [ad.AttrDict(r) for r in all_results if r["type"] == "relation" and r["tags"]["type"] == "route"]
    logging.debug("Found %d relations with ids %s" % (len(relations), [r["id"] for r in relations]))
    stops = [ad.AttrDict(r) for r in all_results if r["type"] != "relation"]
    logging.debug("Found %d stops" % len(stops))
    rel_map = {}
    for relation in relations:
        rel_nodes_ids = [rm.ref for rm in relation.members if (rm.type == "node")]
        rel_stop_node_ids = [rm.ref for rm in relation.members if (rm.type == "node") 
            and (rm.type == "stop")]
        # logging.debug("for relation number of nodes = %d, number of stop nodes = %d" % (len(rel_nodes_ids),
        #                                                            len(rel_stop_node_ids)))
        if len(rel_nodes_ids) == 0:
            logging.debug("route relation with no nodes = %s" % relation["tags"])
        rel_map[relation.id] = rel_nodes_ids
    # logging.debug("rel_map = %s" % rel_map)
    for stop in stops:
        stop["routes"] = []
        for relation in relations:
            rel_nodes_ids = rel_map[relation["id"]]
            if stop.id in rel_nodes_ids:
                stop["routes"].append({"id": relation["id"], "tags": relation["tags"]})
    return stops

# https://gis.stackexchange.com/a/19761
def get_stops_near(loc, distance_in_meters):
    COORDS = "coordinates"
    # 10001.965729km = 90 degrees
    # 1km = 90/10001.965729 degrees
    # 1000m = 90/10001.965729 degrees
    # 1m = 90/(10001.965729 * 1000)
    meter2deg = 90/(10001.965729 * 1000)
    bbox_delta = meter2deg * distance_in_meters
    lon = loc[COORDS][0]
    lat = loc[COORDS][1]
    stops = get_public_transit_stops(lat - bbox_delta, lon - bbox_delta, lat + bbox_delta, lon + bbox_delta)
    logging.debug("Found %d stops" % len(stops))
    for i, stop in enumerate(stops):
        logging.debug("STOP %d: %s" % (i, stop))
    return stops

def get_predicted_transit_mode(start_stops, end_stops):
    """
    This is a set of checks, in decreasing order of confidence.
    The advantage of doing it this way is that we can add more checks as we
    encounter more use cases.
    """
    # https://stackoverflow.com/a/11574353/4040267
    p_start_routes = list(itertools.chain.from_iterable([extract_routes(s) for s in start_stops]))
    p_end_routes = list(itertools.chain.from_iterable([extract_routes(s) for s in end_stops]))

    rel_id_matches = get_rel_id_match(p_start_routes, p_end_routes)
    logging.debug("len(start_routes) = %d, len(end_routes) = %d, len(rel_id_matches) = %d" %
        (len(p_start_routes), len(p_end_routes), len(rel_id_matches)))
    if len(rel_id_matches) > 0:
        return [rim.tags.route for rim in rel_id_matches]

    # Did not find matching routes. Let's see if stops are both "railway",
    # if so, we can mark as TRAIN 
    # TODO: return more complex kinds of railways?
    p_start_train = ["railway" in s.tags for s in start_stops]
    p_end_train = ["railway" in s.tags for s in end_stops]

    logging.debug("len(start_train) = %s, len(end_train) = %s" % (
        (len(p_start_train), len(p_end_train))))
    if is_true(p_start_train) and is_true(p_end_train):
        logging.debug("start and end are both TRAIN, returning TRAIN")
        return ["TRAIN"]

    # Did not find matching routes. Let's see if any stops have a "highway" =
    # "bus_stop" tag
    is_bus_stop = lambda s: "highway" in s.tags and \
                                  s.tags.highway == "bus_stop"

    # Older bus stops sometimes have `route_ref`, which is a `;` separated list
    # of routes. This is now deprecated, but until everything is moved over, we
    # have to use it :(
    # https://help.openstreetmap.org/questions/33325/bus-stops-by-line-bus
    start_bus_stops = [s for s in start_stops if is_bus_stop(s)]
    end_bus_stops = [s for s in end_stops if is_bus_stop(s)]
    logging.debug("%d start stops -> %d start bus stops, %d end stops -> %d end bus stops" % (len(start_stops), len(start_bus_stops), len(end_stops), len(end_bus_stops)))
    start_bus_route_ref = create_routes_from_ref(start_bus_stops)
    end_bus_route_ref = create_routes_from_ref(end_bus_stops)
    route_ref_matches = get_rel_id_match(start_bus_route_ref, end_bus_route_ref)
    logging.debug("len(start_bus_route_ref) = %d, len(end_bus_route_ref) = %d, len(rel_id_matches) = %d" %
        (len(start_bus_route_ref), len(end_bus_route_ref), len(route_ref_matches)))
    if len(route_ref_matches) > 0:
        return [rim.tags.route for rim in route_ref_matches]

    p_start_bus = [is_bus_stop(s) for s in start_stops]
    p_end_bus = [is_bus_stop(s) for s in end_stops]

    # If there are no route refs either, let's be a bit careful, since without
    # routes, we could end up with a lot of false positives.
    # In general, in areas with a high density of routes, we expect to have
    # good OSM coverage with route information, so the first checks will match
    # And if we fall through to here, we probably don't have a dense public
    # transit network. So to avoid misclassifying car trips as bus, let's check
    # that the density is low.
    logging.debug("len(start_bus) = %s, len(end_bus) = %s" % (
        (len(p_start_bus), len(p_end_bus))))
    if is_true(p_start_bus) and is_true(p_end_bus):
        # Query for bus stops in the surrounding area and compare the density
        # overall versus the density here
        # If the density of bus stops is much higher here, then the chances are
        # that it is a bus trip since otherwise, by random chance, it should
        # have started anywhere in the space
        logging.debug("Both start and end are bus stops, validating...")
        if validate_simple_bus_stops(start_stops, end_stops):
            logging.debug("Validation succeeded, returning BUS")
            return ['BUS']

    # No public transit matches, return None
    return None

def get_rel_id_match(p_start_routes, p_end_routes):
    logging.debug("About to find matches in lists: %s \n %s" % 
        ([p.id for p in p_start_routes], 
         [p.id for p in p_end_routes]))
    matching_routes = []
    for sr in p_start_routes:
        for er in p_end_routes:
            if sr.id == er.id:
                matching_routes.append(sr)
    logging.debug("matching routes = %s" % [(r.id, r.tags.ref) for r in matching_routes])
    return matching_routes

def extract_routes(stop):
    p_modes = []
    if "routes" in stop:
        for route in stop.routes:
            p_modes.append(route)

    logging.debug("After iterating through routes, potential modes = %s" % 
        [(p.id, p.tags.route) for p in p_modes])

    return p_modes

def create_routes_from_ref(bus_stop_list):
    created_routes = []
    route_ref_bus_stop_list = [s for s in bus_stop_list if "route_ref" in s.tags]
    for s in route_ref_bus_stop_list:
        logging.debug("Splitting route ref %s" % s.tags.route_ref)
        # route_ref is a ; separated list of routes. We want to split them up
        route_list = s.tags.route_ref.split(';')
        for route in route_list:
            # the id of the stop represents the stop, not the route
            # so we create an id from the route
            re = {"id": route,
                   "tags": {"ref": route, "name": route, "route": "bus", "type": "route"}}

            # 'tags': {'bus': 'yes', 'gtfs_id': '0300315', 'gtfs_location_type': '0', 'gtfs_stop_code': '57566', 'highway': 'bus_stop', 'name': 'Addison St:Oxford St', 'network': 'AC Transit', 'public_transport': 'platform', 'ref': '57566', 'route_ref': '65'}
            # #65 bus stop doesn't have an operator tag, only network
            if "operator" in s.tags:
                re["operator"] = s.tags.operator
            elif "network" in s.tags:
                re["operator"] = s.tags.network
            # logging.debug("Converted stop %s + route_ref %s -> route %s" %
            #    (s, route, re))
            created_routes.append(ad.AttrDict(re))
    logging.debug("%d bus stops -> %d bus stops with refs -> %d routes" %
        (len(bus_stop_list), len(route_ref_bus_stop_list), len(created_routes)))
    return created_routes

def is_true(bool_array):
    import functools as ft

    ret_val = ft.reduce(lambda x, y: x or y, bool_array, False)
    logging.debug("is_true(%s) = %s" % (bool_array, ret_val))
    return ret_val

def validate_simple_bus_stops(p_start_stops, p_end_stops):
    is_bus_stop = lambda s: "highway" in s.tags and \
                            s.tags.highway == "bus_stop"

    start_bus_stops = [s for s in p_start_stops if is_bus_stop(s)]
    end_bus_stops = [s for s in p_end_stops if is_bus_stop(s)]

    # at least one of the sides should be sparse
    if len(start_bus_stops) == 1 or len(end_bus_stops) == 1:
        logging.debug("One side is sparse, valid bus stop")
        return True

    logging.debug("Both side are dense, invalid bus stop")
    return False
