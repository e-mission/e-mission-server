import logging
import json
import requests
import attrdict as ad
import itertools
import os
import time

try:
    GEOFABRIK_OVERPASS_KEY = os.environ.get("GEOFABRIK_OVERPASS_KEY")
    url = 'https://overpass.geofabrik.de/' + GEOFABRIK_OVERPASS_KEY + '/'
    print("overpass configured")
except:
    print("overpass not configured, falling back to public overpass api")
    url = "https://lz4.overpass-api.de/"

RETRY = -1

def make_request_and_catch(overpass_query):
    try:
        response = requests.post(url + "api/interpreter", data=overpass_query)
    except requests.exceptions.ChunkedEncodingError as e:
        logging.info("ChunkedEncodingError while creating request %s" % (e))
        time.sleep(10)
        logging.info("Retrying after 10 second sleep")
        return RETRY

    try:
        all_results = response.json()["elements"]
        return all_results
    except json.decoder.JSONDecodeError as e:
        logging.info("Unable to decode response with status_code %s, text %s" %
            (response.status_code, response.text))
        if response.status_code == 429:
            logging.info("Checking when a slot is available")
            response = requests.get(url + "api/status")
            status_string = response.text.split("\n")
            logging.info("status string is %s", status_string)
            try:
                available_slots = int(status_string[4].split(" ")[0])
                if available_slots > 0:
                    logging.info("No need to wait")
                    return RETRY
                # Some api/status returns 0 slots available and then when they will be available
                elif available_slots == 0:
                    min_waiting_time = min(int(status_string[5].split(" ")[5]), int(status_string[6].split(" ")[5]))
                    time.sleep(min_waiting_time)
                    return RETRY
            except ValueError as e:
                # And some api/status directly returns when the slots will be available
                try:
                    min_waiting_time = min(int(status_string[4].split(" ")[5]), int(status_string[5].split(" ")[5]))
                    time.sleep(min_waiting_time)
                    return RETRY
                except ValueError as e:
                    logging.info("Unable to find available slots")
                    all_results = []
        else:
            all_results = []
    return all_results


def get_query_for_bbox(bbox):
    bbox_string = "%s,%s,%s,%s" % bbox
    return f'''
    (
        node["highway"="bus_stop"]({bbox_string});
        node["railway"="station"]({bbox_string});
        node["public_transport"]({bbox_string});
        way["railway"="station"]({bbox_string});
        relation["route"]({bbox_string});
    );
    out body;
    out count;
    >;
    '''


def get_query_for_bboxes(bboxes):
    query = '[out:json][timeout:25];\n'
    for bbox in bboxes:
        query += get_query_for_bbox(bbox)
    return query


def get_public_transit_stops(bboxes):
    overpass_query = get_query_for_bboxes(bboxes)
    call_return = RETRY
    retry_count = 0
    while call_return == RETRY:
        if retry_count > 0:
            logging.info(f"call_return = {call_return}, retrying...")
        call_return = make_request_and_catch(overpass_query)
        logging.info(f"after retry, got {'RETRY' if call_return == RETRY else len(call_return)}...")
        if call_return == RETRY:
            retry_count = retry_count + 1
            logging.info(f"after incrementing, retry_count = {retry_count}...")

    logging.info(f"after all retries, retry_count = {retry_count}, call_return = {'RETRY' if call_return == RETRY else len(call_return)}...")
    all_results = call_return

    locs_results = []
    current_loc_results = []
    for result in all_results:
        if result.get("type") == "count":
            locs_results.append(current_loc_results)
            current_loc_results = []
        else:
            current_loc_results.append(result)

    return [get_stops_from_results(loc_results) for loc_results in locs_results]


def get_stops_from_results(results):
    relations = [ad.AttrDict(r) for r in results if r["type"] == "relation" and r["tags"]["type"] == "route"]
    logging.debug("Found %d relations with ids %s" % (len(relations), [r["id"] for r in relations]))
    stops = [ad.AttrDict(r) for r in results if r["type"] != "relation"]
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

def get_stops_near(coord_pairs, distance_in_meters):
    # https://gis.stackexchange.com/a/19761
    # 10001.965729km = 90 degrees
    # 1km = 90/10001.965729 degrees
    # 1000m = 90/10001.965729 degrees
    # 1m = 90/(10001.965729 * 1000)
    meter2deg = 90/(10001.965729 * 1000)
    bbox_delta = meter2deg * distance_in_meters
    bboxes = [
        (lat - bbox_delta, lon - bbox_delta,
         lat + bbox_delta, lon + bbox_delta)
        for [lon, lat] in coord_pairs
    ]
    locs_stops = get_public_transit_stops(bboxes)
    for i, loc_stops in enumerate(locs_stops):
        logging.debug(f"Found {len(loc_stops)} stops for bbox {bboxes[i]}")
    return locs_stops

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
    # if so, we can mark as LIGHT_RAIL, TRAIN, TRAM or SUBWAY
    p_start_train = [extract_railway_modes(s.tags) for s in start_stops]
    p_start_train = set(itertools.chain.from_iterable(set(i) for i in p_start_train))
    p_end_train = [extract_railway_modes(s.tags) for s in end_stops]
    p_end_train = set(itertools.chain.from_iterable(set(i) for i in p_end_train))
    logging.debug("len(start_train) = %d, len(end_train) = %d" %
        (len(p_start_train), len(p_end_train)))
    if len(p_start_train) > 0 and len(p_end_train) > 0:
        p_intersection_train = p_start_train & p_end_train
        p_intersection_train = list(p_intersection_train)
        logging.debug("Start and end have both " + str(p_intersection_train) + ", returning " + str(p_intersection_train))
        return p_intersection_train
    
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
    train_mode_list = ['funicular', 'miniature', 'rail', 'railway',
    'light_rail', 'subway', 'monorail', 'tram', 'aerialway', 'tracks']
    logging.debug("About to find matches in lists: %s \n %s" % 
        ([p.id for p in p_start_routes], 
         [p.id for p in p_end_routes]))
    matching_routes = []
    for sr in p_start_routes:
        for er in p_end_routes:
            if sr.id == er.id:
                matching_routes.append(sr)
            elif sr.tags.route in train_mode_list and er.tags.route in train_mode_list and sr.tags.route == er.tags.route:
                if "network" in sr.tags and "network" in er.tags:
                    if sr.tags.network == er.tags.network:
                        logging.debug("network matches between %d and  %d", sr.id,er.id)
                        matching_routes.append(sr)
    logging.debug("matching routes = %s" % [(r.id,
        r.tags.ref if "ref" in r.tags else r.tags.name) for r in matching_routes])
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
    is_no_route_bus_stop = lambda s: "highway" in s.tags and \
                                     s.tags.highway == "bus_stop" and \
                                     "route_ref" not in s.tags and \
                                     "routes" not in s

    start_bus_stops = [s for s in p_start_stops if is_no_route_bus_stop(s)]
    end_bus_stops = [s for s in p_end_stops if is_no_route_bus_stop(s)]

    # at least one of the sides should be sparse
    if len(start_bus_stops) == 1 or len(end_bus_stops) == 1:
        logging.debug("One side is sparse, valid bus stop")
        return True

    logging.debug("Both side are dense, invalid bus stop")
    return False

def extract_railway_modes(stop):
    p_modes = []
    if "railway" in stop:
        if "subway" in stop:
            p_modes.append("SUBWAY")
        if "train" in stop:
            p_modes.append("TRAIN")
        if "tram" in stop:
            p_modes.append("TRAM")
        if "light_rail" in stop:
            p_modes.append("LIGHT_RAIL")

    logging.debug("After extracting data from tags, potential modes = %s" %
        [p for p in p_modes])

    return p_modes
