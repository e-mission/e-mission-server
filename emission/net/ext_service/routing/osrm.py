import logging
import json
import requests
import polyline.codec as pc

try:
    osrm_file = open("conf/net/ext_service/osrm.json")
    osrm_config = json.load(osrm_file)
    osrm_file.close()
    OSRM_HOST = osrm_config["routing_host"]
    OSRM_ROUTES = osrm_config["paths"]
except Exception as e:
    print("OSRM not configured, routing is not possible")
    logging.exception(e)

###
# mode is expected to be a ecwp.PredictedModeTypes mode object
# waypoints are an array of geojson [lng, lat] coordinates
# params is a dictionary that is passed unchanged to the query
# returns a JSON result
###

def get_route(mode, waypoints, params):
    logging.debug("waypoints = %s..." % waypoints[0:3])
    route_coords_string = ";".join([",".join([str(lon), str(lat)]) for [lon, lat] in waypoints])
    logging.debug("route_coords_string = %s" % route_coords_string[0:30])
    # Now that we want to re-use this module outside of the e-mission server code
    # the mode parameter can either be a PredictedModeTypes object or a string
    # maybe eventually we can standardize on only a string, after we split apart
    # the monolithic e-mission server into multiple modules
    # but let us support both for now
    mode_name = mode.name if "name" in mode else mode
    url_to_query = OSRM_HOST + "/" + OSRM_ROUTES[mode_name] + "/" + route_coords_string
    response = requests.get(url_to_query, params=params)
    logging.debug("Call to URL %s returns %s" % (response.url, response))
    return response.json()

def get_points_from_route_result(route_response_json):
    string_to_encode = route_response_json["routes"][0]["geometry"]
    decoded_geometry = pc.PolylineCodec().decode(string_to_encode)
    logging.debug("Decoding %s... -> %s..." % (string_to_encode[0:30],
        decoded_geometry[0:3]))
    return decoded_geometry

def get_route_points(mode, waypoints, params):
    response_json = get_route(mode, waypoints, params)
    return get_points_from_route_result(response_json)
