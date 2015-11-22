import logging
import geojson as gj
import copy
import attrdict as ad
import pandas as pd

import emission.storage.timeseries.abstract_timeseries as esta

import emission.storage.decorations.trip_queries as esdt
import emission.storage.decorations.section_queries as esds
import emission.storage.decorations.timeline as esdtl

import emission.core.wrapper.location as ecwl
import emission.core.wrapper.entry as ecwe

# TODO: Move this to the section_features class instead
import emission.analysis.intake.cleaning.location_smoothing as eaicl

def _del_non_derializable(prop_dict, extra_keys):
    if "user_id" in prop_dict:
        # It is not in the location entries that are created from the data df
        del prop_dict["user_id"]
    del prop_dict["_id"]
    for key in extra_keys:
        if key in prop_dict:
            del prop_dict[key]

def _stringify_foreign_key(prop_dict, key_names):
    for key_name in key_names:
        if hasattr(prop_dict, key_name):
            setattr(prop_dict, key_name, str(getattr(prop_dict,key_name)))

def location_to_geojson(location):
    """
    Converts a location wrapper object into geojson format.
    This is pretty easy - it is a point.
    Since we have other properties that we care about, we make it a feature.
    Then, all the other stuff goes directly into the properties since the wrapper is a dict too!
    :param location: the location object
    :return: a geojson version of the location. the object is of type "Feature".
    """
    try:
        ret_feature = gj.Feature()
        ret_feature.id = str(location.get_id())
        ret_feature.geometry = location.loc
        ret_feature.properties = copy.copy(location)
        ret_feature.properties["feature_type"] = "location"
        _del_non_derializable(ret_feature.properties, ["loc"])
        return ret_feature
    except Exception, e:
        logging.exception(("Error while converting object %s" % location))
        raise e

def place_to_geojson(place):
    """
    Converts a place wrapper object into geojson format.
    This is also pretty easy - it is just a point.
    Since we have other properties that we care about, we make it a feature.
    Then, all the other stuff goes directly into the properties since the wrapper is a dict too!
    :param place: the place object
    :return: a geojson version of the place. the object is of type "Feature".
    """

    ret_feature = gj.Feature()
    ret_feature.id = str(place.get_id())
    ret_feature.geometry = place.location
    ret_feature.properties = copy.copy(place)
    ret_feature.properties["feature_type"] = "place"
    # _stringify_foreign_key(ret_feature.properties, ["ending_trip", "starting_trip"])
    _del_non_derializable(ret_feature.properties, ["location"])
    return ret_feature


def stop_to_geojson(stop):
    """
    Converts a stop wrapper object into geojson format.
    This is also pretty easy - it is just a point.
    Since we have other properties that we care about, we make it a feature.
    Then, all the other stuff goes directly into the properties since the wrapper is a dict too!
    :param stop: the stop object
    :return: a geojson version of the stop. the object is of type "Feature".
    """

    ret_feature = gj.Feature()
    ret_feature.id = str(stop.get_id())
    ret_feature.geometry = gj.LineString()
    ret_feature.geometry.coordinates = [stop["enter_loc"]["coordinates"], stop["exit_loc"]["coordinates"]]
    ret_feature.properties = copy.copy(stop)
    ret_feature.properties["feature_type"] = "stop"

    # _stringify_foreign_key(ret_feature.properties, ["ending_section", "starting_section", "trip_id"])
    _del_non_derializable(ret_feature.properties, ["location"])
    return ret_feature

def section_to_geojson(section, tl):
    """
    This is the trickiest part of the visualization.
    The section is basically a collection of points with a line through them.
    So the representation is a feature in which one feature which is the line, and one feature collection which is the set of point features.
    :param section: the section to be converted
    :return: a feature collection which is the geojson version of the section
    """

    ts = esta.TimeSeries.get_time_series(section.user_id)
    entry_it = ts.find_entries(["background/filtered_location"], esds.get_time_query_for_section(section.get_id()))
    # points_df = ts.get_data_df("background/filtered_location", esds.get_time_query_for_section(section.get_id()))
    # points_df = points_df.drop("elapsedRealTimeNanos", axis=1)
    # logging.debug("points_df.columns = %s" % points_df.columns)

    # TODO: Decide whether we want to use Rewrite to use dataframes throughout instead of python arrays.
    # dataframes insert nans. We could use fillna to fill with default values, but if we are not actually
    # using dataframe features here, it is unclear how much that would help.
    feature_array = []
    section_location_array = [ecwl.Location(ts._to_df_entry(entry)) for entry in entry_it]

    logging.debug("first element in section_location_array = %s" % section_location_array[0])

    # Fudge the end point so that we don't have a gap because of the ts != write_ts mismatch
    # TODO: Fix this once we are able to query by the data timestamp instead of the metadata ts
    if section_location_array[-1].loc != section.end_loc:
        last_loc_doc = ts.get_entry_at_ts("background/filtered_location", "data.ts", section.end_ts)
        last_loc_data = ecwe.Entry(last_loc_doc).data
        last_loc_data["_id"] = last_loc_doc["_id"]
        section_location_array.append(last_loc_data)
        logging.debug("Adding new entry %s to fill the end point gap between %s and %s"
            % (last_loc_data.loc, section_location_array[-2].loc, section.end_loc))

    # Find the list of points to filter
    filtered_points_entry_doc = ts.get_entry_at_ts("analysis/smoothing", "data.section",
                                                               section.get_id())
    if filtered_points_entry_doc is None:
        logging.debug("No filtered_points_entry, returning unchanged array")
        filtered_section_location_array = section_location_array
    else:
        # TODO: Figure out how to make collections work for the wrappers and then change this to an Entry
        filtered_points_entry = ad.AttrDict(filtered_points_entry_doc)
        filtered_point_list = list(filtered_points_entry.data.deleted_points)
        logging.debug("deleting %s points from section points" % len(filtered_point_list))
        filtered_section_location_array = [l for l in section_location_array if l.get_id() not in filtered_point_list]

    with_speeds = eaicl.add_dist_heading_speed(pd.DataFrame(filtered_section_location_array))
    speeds = list(with_speeds.speed)
    distances = list(with_speeds.distance)
    for idx, row in with_speeds.iterrows():
        # TODO: Remove instance of setting value without going through wrapper class
        filtered_section_location_array[idx]["speed"] = row["speed"]
        filtered_section_location_array[idx]["distance"] = row["distance"]
    points_feature_array = [location_to_geojson(l) for l in filtered_section_location_array]

    points_line_feature = point_array_to_line(filtered_section_location_array)
    # If this is the first section, we already start from the trip start. But we actually need to start from the
    # prior place. Fudge this too. Note also that we may want to figure out how to handle this properly in the model
    # without needing fudging. TODO: Unclear how exactly to do this
    if section.start_stop is None:
        # This is the first section. So we need to find the start place of the parent trip
        parent_trip = tl.get_object(section.trip_id)
        start_place_of_parent_trip = tl.get_object(parent_trip.start_place)
        points_line_feature.geometry.coordinates.insert(0, start_place_of_parent_trip.location.coordinates)

    for i, point_feature in enumerate(points_feature_array):
        point_feature.properties["idx"] = i

    points_line_feature.id = str(section.get_id())
    points_line_feature.properties = copy.copy(section)
    points_line_feature.properties["feature_type"] = "section"
    points_line_feature.properties["sensed_mode"] = str(points_line_feature.properties.sensed_mode)
    points_line_feature.properties["distance"] = sum(distances)
    points_line_feature.properties["speeds"] = speeds
    points_line_feature.properties["distances"] = distances

    _del_non_derializable(points_line_feature.properties, ["start_loc", "end_loc"])

    feature_array.append(gj.FeatureCollection(points_feature_array))
    feature_array.append(points_line_feature)

    return gj.FeatureCollection(feature_array)

def point_array_to_line(point_array):
    points_line_string = gj.LineString()
    # points_line_string.coordinates = [l.loc.coordinates for l in filtered_section_location_array]
    points_line_string.coordinates = []

    for l in point_array:
        # logging.debug("About to add %s to line_string " % l)
        points_line_string.coordinates.append(l.loc.coordinates)
    
    points_line_feature = gj.Feature()
    points_line_feature.geometry = points_line_string
    return points_line_feature    

def trip_to_geojson(trip, tl):
    """
    Trips are the main focus of our current visualization, so they are most complex.
    Each trip is represented as a feature collection with the following features:
    - two features for the start and end places
    - features for each stop in the trip
    - features for each section in the trip

    :param trip: the trip object to be converted
    :param tl: the timeline used to retrieve related objects
    :return: the geojson version of the trip
    """

    feature_array = []
    curr_start_place = tl.get_object(trip.start_place)
    curr_end_place = tl.get_object(trip.end_place)
    start_place_geojson = place_to_geojson(curr_start_place)
    start_place_geojson["properties"]["feature_type"] = "start_place"
    feature_array.append(start_place_geojson)

    end_place_geojson = place_to_geojson(curr_end_place)
    end_place_geojson["properties"]["feature_type"] = "end_place"
    feature_array.append(end_place_geojson)

    trip_tl = esdt.get_timeline_for_trip(trip.user_id, trip.get_id())
    stops = trip_tl.places
    for stop in stops:
        feature_array.append(stop_to_geojson(stop))

    trip_distance = 0
    for i, section in enumerate(trip_tl.trips):
        # TODO: figure out whether we should do this at the model.
        # The first section starts with the start of the trip. But the trip itself starts at the first
        # point where we exit the geofence, not at the start place. That is because we don't really know when
        # we left the start place. We can fix this in the model through interpolation. For now, we assume that the
        # gap between the real departure time and the time that the trip starts is small, and just combine it here.
        section_gj = section_to_geojson(section, tl)
        feature_array.append(section_gj)
        # import bson.json_util as bju
        # for f in section_gj.features:
        #     logging.debug("Section has feature %s" % bju.dumps(f))
        # TODO: Fix me to use the wrapper
        section_distance = [f["properties"]["distance"] for f in section_gj.features if
            f.type == "Feature" and f.geometry.type == "LineString"]
        logging.debug("found distance %s for section %s" % (section_distance, section.get_id()))
        trip_distance = trip_distance + sum(section_distance)

    trip_geojson = gj.FeatureCollection(features=feature_array, properties=trip)
    trip_geojson.id = str(trip.get_id())
    trip_geojson.properties["feature_type"] = "trip"
    trip_geojson.properties["distance"] = trip_distance
    return trip_geojson

def get_geojson_for_range(user_id, start_ts, end_ts):
    geojson_list = []
    tl = esdtl.get_timeline(user_id, start_ts, end_ts)
    tl.fill_start_end_places()

    for trip in tl.trips:
        try:
            trip_geojson = trip_to_geojson(trip, tl)
            # If the trip has no sections, it will have exactly two points - one for the start place and
            # one for the stop place. If a trip has no sections, let us filter it out here because it is
            # annoying to the user. But in that case, we need to merge the places.
            # Let's make that a TODO after getting everything else to work.
            if len(trip_geojson.features) == 2:
                logging.info("Skipping zero section trip %s with distance %s (should be zero)" %
                             (trip, trip_geojson.properties["distance"]))
            else:
                geojson_list.append(trip_geojson)
        except KeyError, e:
            # We ran into key errors while dealing with mixed filter trips.
            # I think those should be resolved for now, so we can raise the error again
            # But if this is preventing us from making progress, we can comment out the raise
            logging.exception("Found key error %s while processing trip %s" % (e, trip))
            raise e
        except Exception, e:
            logging.exception("Found error %s while processing trip %s" % (e, trip))
            raise e

    return geojson_list    
    
def get_all_points_for_range(user_id, key, start_ts, end_ts):
    import emission.net.usercache.abstract_usercache as enua
#     import emission.core.wrapper.location as ecwl 
    
    tq = enua.UserCache.TimeQuery("write_ts", start_ts, end_ts)
    ts = esta.TimeSeries.get_time_series(user_id)
    entry_it = ts.find_entries([key], tq)
    points_array = [ecwl.Location(ts._to_df_entry(entry)) for entry in entry_it]
    
    points_feature_array = [location_to_geojson(l) for l in points_array]
    print ("Found %d points" % len(points_feature_array))
    
    feature_array = []
    feature_array.append(gj.FeatureCollection(points_feature_array))
    feature_array.append(point_array_to_line(points_array))
    feature_coll = gj.FeatureCollection(feature_array)
        
    return feature_coll
