import pandas as pd
import folium.folium as folium
import itertools
import numpy as np
import logging
import geojson as gj
import copy
import attrdict as ad
from functional import seq

# import emission.analysis.classification.cleaning.location_smoothing as ls
import bson.json_util as bju

import emission.storage.decorations.location_queries as lq
import emission.storage.decorations.trip_queries as esdt
import emission.storage.decorations.place_queries as esdp
import emission.storage.decorations.stop_queries as esds
import emission.storage.decorations.section_queries as esdsc

import emission.storage.timeseries.abstract_timeseries as esta

import emission.core.wrapper.stop as ecws
import emission.core.wrapper.section as ecwsc

import emission.analysis.plotting.geojson.geojson_feature_converter as gfc
import emission.analysis.plotting.leaflet_osm.folium_geojson_plugin as fgjp

import emission.net.usercache.abstract_usercache as enua
import emission.net.api.usercache as enau

all_color_list = ['black', 'brown', 'blue', 'chocolate', 'cyan', 'fuschia', 'green', 'lime', 'magenta', 'navy', 'pink', 'purple', 'red', 'snow', 'yellow']
sel_color_list = ['black', 'blue', 'chocolate', 'cyan', 'fuschia', 'green', 'lime', 'magenta', 'pink', 'purple', 'red', 'yellow']

def df_to_string_list(df):
    """
    Convert the input df into a list of strings, suitable for using as popups in a map.
    This is a utility function.
    """
    # print "Converting df with size %s to string list" % df.shape[0]
    array_list = df.to_dict(orient='records')
    return [str(line) for line in array_list]

def get_maps_for_range(user_id, start_ts, end_ts):
    map_list = []
    geojson_list = gfc.get_geojson_for_range(user_id, start_ts, end_ts)
    return get_maps_for_geojson_list(geojson_list)

def get_maps_for_usercache(user_id):
    data_to_phone = seq(enau.sync_server_to_phone(user_id))
    logging.debug("Before pipeline, trips to phone list has length %d" % len(data_to_phone.to_list()))
    logging.debug("keys are %s" % data_to_phone.map(lambda e: ad.AttrDict(e).metadata.key))
    trips_to_phone = data_to_phone.map(lambda e: ad.AttrDict(e))\
                                    .filter(lambda e: e.metadata.key.startswith("diary/trips")) \
                                    .map(lambda e: e.data)
    logging.debug("After pipeline, trips to phone list has length %d" % len(trips_to_phone.to_list()))
    assert(len(trips_to_phone.to_list()) == 1)
    # logging.debug("trips_to_phone = %s" % trips_to_phone)
    return get_maps_for_geojson_list(trips_to_phone[0])

def get_maps_for_geojson_list(trip_geojson_list):
    map_list = []
    for trip_doc in trip_geojson_list:
        # logging.debug(trip_geojson)
        trip_geojson = ad.AttrDict(trip_doc)
        logging.debug("centering based on start = %s, end = %s " % (trip_geojson.features[0], trip_geojson.features[1]))
        flipped_midpoint = lambda(p1, p2): [(p1.coordinates[1] + p2.coordinates[1])/2,
                                            (p1.coordinates[0] + p2.coordinates[0])/2]

        curr_map = folium.Map(flipped_midpoint((trip_geojson.features[0].geometry,
                                                trip_geojson.features[1].geometry)))
        curr_plugin = fgjp.FoliumGeojsonPlugin(dict(trip_geojson))
        curr_map.add_plugin(curr_plugin)
        map_list.append(curr_map)
    return map_list
    
def flipped(coord):
    return (coord[1], coord[0])
    
def get_center_for_map(coords):
    # logging.debug(trip_geojson)
    midpoint = lambda(p1, p2): [(p1[0] + p2[0])/2,
                                (p1[1] + p2[1])/2]
    if len(coords) == 0:
        return None
    if len(coords) == 1:
        return flipped(coords)
    if len(coords) > 0:
        logging.debug("Getting midpoint of %s and %s" % (coords[0], coords[-1]))
        return flipped(midpoint((coords[0], coords[-1])))
    
def get_maps_for_geojson_unsectioned(feature_list):
    map_list = []
    for feature in feature_list:
        # logging.debug("Getting map for feature %s" % bju.dumps(feature))
        feature_coords = list(get_coords(feature))
        # feature_coords = list(gj.utils.coords(feature))
        curr_map = folium.Map(get_center_for_map(feature_coords))
        curr_plugin = fgjp.FoliumGeojsonPlugin(dict(feature))
        curr_map.add_plugin(curr_plugin)
        map_list.append(curr_map)
    return map_list

def get_coords(feature):
    # logging.debug("Getting coordinates for feature %s" % bju.dumps(feature))
    if feature["type"] == "FeatureCollection":
        retVal = []
        for f in feature["features"]:
            retVal.extend(get_coords(f))
        return retVal
    else:
        return gj.utils.coords(feature)

def get_maps_for_range_old(user_id, start_ts, end_ts):
    # First, get the timeline for that range.
    ts = esta.TimeSeries.get_time_series(user_id)
    trip_list = esdt.get_trips(user_id, enua.UserCache.TimeQuery("start_ts", start_ts, end_ts))
    # TODO: Should the timeline support random access as well?
    # If it did, we wouldn't need this additional map
    # I think that it would be good to support a doubly linked list, i.e. prev and next in addition
    # to the iteration interface
    place_list = esdp.get_places(user_id, enua.UserCache.TimeQuery("exit_ts", start_ts, end_ts))
    place_list = place_list + (esdp.get_places(user_id, enua.UserCache.TimeQuery("enter_ts", start_ts, end_ts)))
    place_map = dict([(p.get_id(), p) for p in place_list])
    map_list = []
    flipped_midpoint = lambda(p1, p2): [(p1.coordinates[1] + p2.coordinates[1])/2,
                                        (p1.coordinates[0] + p2.coordinates[0])/2]
    for i, trip in enumerate(trip_list):
        logging.debug("-" * 20 + trip.start_fmt_time + "=>" + trip.end_fmt_time
                      + "(" + str(trip.end_ts - trip.start_ts) + ")")
        if (len(esdt.get_sections_for_trip(user_id, trip.get_id())) == 0 and
            len(esdt.get_stops_for_trip(user_id, trip.get_id())) == 0):
            logging.debug("Skipping trip because it has no stops and no sections")
            continue

        start_point = gj.GeoJSON.to_instance(trip.start_loc)
        end_point = gj.GeoJSON.to_instance(trip.end_loc)
        curr_map = folium.Map(flipped_midpoint((start_point, end_point)))
        map_list.append(curr_map)
        logging.debug("About to display places %s and %s" % (trip.start_place, trip.end_place))
        update_place(curr_map, trip.start_place, place_map, marker_color='green')
        update_place(curr_map, trip.end_place, place_map, marker_color='red')
        # TODO: Should get_timeline_for_trip work on a trip_id or on a trip object
        # it seems stupid to convert trip object -> id -> trip object
        curr_trip_timeline = esdt.get_timeline_for_trip(user_id, trip.get_id())
        for i, trip_element in enumerate(curr_trip_timeline):
            # logging.debug("Examining element %s of type %s" % (trip_element, type(trip_element)))
            if type(trip_element) == ecws.Stop:
                time_query = esds.get_time_query_for_stop(trip_element.get_id())
                logging.debug("time_query for stop %s = %s" % (trip_element, time_query))
                stop_points_df = ts.get_data_df("background/filtered_location", time_query)
                # logging.debug("stop_points_df.head() = %s" % stop_points_df.head())
                if len(stop_points_df) > 0:
                    update_line(curr_map, stop_points_df, line_color = sel_color_list[-1],
                                popup="%s -> %s" % (trip_element.enter_fmt_time, trip_element.exit_fmt_time))
            else:
                assert(type(trip_element) == ecwsc.Section)
                time_query = esdsc.get_time_query_for_section(trip_element.get_id())
                logging.debug("time_query for section %s = %s" %
                              (trip_element, "[%s,%s,%s]" % (time_query.timeType, time_query.startTs, time_query.endTs)))
                section_points_df = ts.get_data_df("background/filtered_location", time_query)
                logging.debug("section_points_df.tail() = %s" % section_points_df.tail())
                if len(section_points_df) > 0:
                    update_line(curr_map, section_points_df, line_color = sel_color_list[trip_element.sensed_mode.value],
                                popup="%s (%s -> %s)" % (trip_element.sensed_mode, trip_element.start_fmt_time,
                                                         trip_element.end_fmt_time))
                else:
                    logging.warn("found no points for section %s" % trip_element)
    return map_list


def update_place(curr_map, place_id, place_map, marker_color='blue'):
    if place_id is not None and place_id in place_map:
        place = place_map[place_id]
        logging.debug("Retrieved place %s" % place)
        if hasattr(place, "location"):
            coords = copy.copy(place.location.coordinates)
            coords.reverse()
            logging.debug("Displaying place at %s" % coords)
            curr_map.simple_marker(location=coords, popup=str(place), marker_color=marker_color)
        else:
            logging.debug("starting place has no location, skipping")
    else:
        logging.warn("place not mapped because place_id = %s and place_id in place_map = %s" % (place_id, place_id in place_map))

def update_line(currMap, line_points, line_color = None, popup=None):
    currMap.div_markers(line_points[['latitude', 'longitude']].as_matrix().tolist(),
        df_to_string_list(line_points), marker_size=5)
    currMap.line(line_points[['latitude', 'longitude']].as_matrix().tolist(),
        line_color = line_color,
        popup = popup)


########################## 
# Everything below this line is from the time when we were evaluating
# segmentation and can potentially be deleted. It is also likely to have bitrotted.
#  Let's hold off a bit on that until we have the replacement, though
##########################

def get_map_list(df, potential_splits):
    mapList = []
    potential_splits_list = list(potential_splits)
    for start, end in zip(potential_splits_list, potential_splits_list[1:]):
        trip = df[start:end]
        print "Considering trip from %s to %s because start = %d and end = %d" % (df.formatted_time.loc[start], df.formatted_time.loc[end], start, end)
        if end - start < 4:
            # If there are only 3 entries, that means that there is only one
            # point other than the start and the end, bail
            print "Ignoring trip from %s to %s because start = %d and end = %d" % (df.formatted_time.loc[start], df.formatted_time.loc[end], start, end)
            continue
        mapList.append(get_map(trip))
    return mapList

def get_map_list_after_segmentation(section_map, outlier_algo = None, filter_algo = None):
    mapList = []
    for trip, section_list in section_map:
        logging.debug("%s %s -> %s %s" % ("=" * 20, trip.start_time, trip.end_time, "=" * 20))
        trip_df = lq.get_points_for_section(trip)
        curr_map = folium.Map([trip_df.mLatitude.mean(), trip_df.mLongitude.mean()])
        last_section_end = None
        for (i, section) in enumerate(section_list):
            logging.debug("%s %s: %s -> %s %s" % 
                ("-" * 20, i, section.start_time, section.end_time, "-" * 20))
            raw_section_df = trip_df[np.logical_and(trip_df.mTime >= section.start_ts,
                                                trip_df.mTime <= section.end_ts)]
            section_df = ls.filter_points(raw_section_df, outlier_algo, filter_algo)
            if section_df.shape[0] == 0:
                logging.info("Found empty df! skipping...")
                continue
            logging.debug("for section %s, section_df.shape = %s, formatted_time.head() = %s" %
                    (section, section_df.shape, section_df["formatted_time"].head()))
            update_map(curr_map, section_df, line_color = sel_color_list[section.activity.value],
                        popup = "%s" % (section.activity))
            if section_df.shape[0] > 0:
                curr_section_start = section_df.iloc[0]
                if i != 0 and last_section_end is not None:
                    # We want to join this to the previous section.
                    curr_map.line([[last_section_end.mLatitude, last_section_end.mLongitude],
                                   [curr_section_start.mLatitude, curr_section_start.mLongitude]],
                                   line_color = sel_color_list[-1],
                                   popup = "%s -> %s" % (section_list[i-1].activity, section.activity))
                last_section_end = section_df.iloc[-1]
        mapList.append(curr_map)
    return mapList

def get_map(section_points, line_color = None, popup=None):
    currMap = folium.Map([section_points.mLatitude.mean(), section_points.mLongitude.mean()])
    update_map(currMap, section_points, line_color, popup)
    return currMap

def update_map(currMap, section_points, line_color = None, popup=None):
    currMap.div_markers(section_points[['mLatitude', 'mLongitude']].as_matrix().tolist(),
        df_to_string_list(section_points), marker_size=5)
    currMap.line(section_points[['mLatitude', 'mLongitude']].as_matrix().tolist(),
        line_color = line_color,
        popup = popup)

def evaluate_filtering(section_list, outlier_algos, filtering_algos):
    """
    TODO: Is this the best place for this? If not, what is?
    It almost seems like we need to have a separate evaluation module that is
    separate from the plotting and the calculation modules.
    But then, what is the purpose of this module?
    """
    nCols = 2 + len(outlier_algos) * len(filtering_algos)
    nRows = len(section_list)

    map_list = []

    for section in section_list:
        curr_compare_list = []
        section_df = ls.get_section_points(section)
        curr_compare_list.append(get_map(section_df))
        curr_compare_list.append(get_map(ls.filter_points(section_df, None, None)))
        for (oa, fa) in itertools.product(outlier_algos, filtering_algos):
            curr_filtered_df = ls.filter_points(section_df, oa, fa)
            print ("After filtering with %s, %s, size is %s" % (oa, fa, curr_filtered_df.shape))
            if "activity" in section:
                curr_compare_list.append(get_map(curr_filtered_df,
                                            line_color = sel_color_list[section.activity.value],
                                                 popup = "%s" % (section.activity)))
            else:
                curr_compare_list.append(get_map(curr_filtered_df))
        assert(len(curr_compare_list) == nCols)
        map_list.append(curr_compare_list)
    assert(len(map_list) == nRows)
    return map_list
