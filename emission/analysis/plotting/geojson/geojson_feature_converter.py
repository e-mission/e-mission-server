import logging
import geojson as gj
import copy
import attrdict as ad

import emission.storage.timeseries.abstract_timeseries as esta

import emission.storage.decorations.trip_queries as esdt
import emission.storage.decorations.section_queries as esds
import emission.storage.decorations.timeline as esdtl

import emission.core.wrapper.location as ecwl
import emission.core.wrapper.entry as ecwe

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
    _del_non_derializable(ret_feature.properties, ["location", "ending_trip", "starting_trip"])
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
    ret_feature.geometry = stop.location
    ret_feature.properties = copy.copy(stop)
    ret_feature.properties["feature_type"] = "stop"

    # _stringify_foreign_key(ret_feature.properties, ["ending_section", "starting_section", "trip_id"])
    _del_non_derializable(ret_feature.properties, ["location", "ending_section", "starting_section", "trip_id"])
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
    points_df = ts.get_data_df("background/filtered_location", esds.get_time_query_for_section(section.get_id()))
    logging.debug("points_df.columns = %s" % points_df.columns)

    feature_array = []
    section_location_array = [ecwl.Location(row) for idx, row in points_df.iterrows()]

    logging.debug("first element in section_location_array = %s" % section_location_array[0])

    # Fudge the end point so that we don't have a gap because of the ts != write_ts mismatch
    if section_location_array[-1].loc != section.end_loc:
        last_loc_doc = ts.get_entry_at_ts("background/filtered_location", "data.ts", section.end_ts)
        last_loc_data = ecwe.Entry(last_loc_doc).data
        last_loc_data["_id"] = last_loc_doc["_id"]
        section_location_array.append(last_loc_data)
        logging.debug("Adding new entry %s to fill the end point gap" % last_loc_data)

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

    points_feature_array = [location_to_geojson(l) for l in filtered_section_location_array]

    points_line_string = gj.LineString()
    # points_line_string.coordinates = [l.loc.coordinates for l in filtered_section_location_array]
    points_line_string.coordinates = []

    for l in filtered_section_location_array:
        logging.debug("About to add %s to line_string " % l)
        points_line_string.coordinates.append(l.loc.coordinates)

    # If this is the first section, we already start from the trip start. But we actually need to start from the
    # prior place. Fudge this too. Note also that we may want to figure out how to handle this properly in the model
    # without needing fudging. TODO: Unclear how exactly to do this
    if section.start_stop is None:
        # This is the first section. So we need to find the start place of the parent trip
        parent_trip = tl.get_object(section.trip_id)
        start_place_of_parent_trip = tl.get_object(parent_trip.start_place)
        points_line_string.coordinates.insert(0, start_place_of_parent_trip.location.coordinates)

    for i, point_feature in enumerate(points_feature_array):
        point_feature.properties["idx"] = i

    points_line_feature = gj.Feature()
    points_line_feature.id = str(section.get_id())
    points_line_feature.geometry = points_line_string
    points_line_feature.properties = copy.copy(section)
    points_line_feature.properties["feature_type"] = "section"
    points_line_feature.properties["sensed_mode"] = str(points_line_feature.properties.sensed_mode)

    # _stringify_foreign_key(points_line_feature.properties, ["start_stop", "end_stop", "trip_id"])
    _del_non_derializable(points_line_feature.properties, ["start_loc", "end_loc", "start_stop", "end_stop", "trip_id"])

    feature_array.append(gj.FeatureCollection(points_feature_array))
    feature_array.append(points_line_feature)

    return gj.FeatureCollection(feature_array)


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

    for i, section in enumerate(trip_tl.trips):
        # TODO: figure out whether we should do this at the model.
        # The first section starts with the start of the trip. But the trip itself starts at the first
        # point where we exit the geofence, not at the start place. That is because we don't really know when
        # we left the start place. We can fix this in the model through interpolation. For now, we assume that the
        # gap between the real departure time and the time that the trip starts is small, and just combine it here.
        feature_array.append(section_to_geojson(section, tl))
    return gj.FeatureCollection(features=feature_array)


def get_geojson_for_range(user_id, start_ts, end_ts):
    geojson_list = []
    tl = esdtl.get_timeline(user_id, start_ts, end_ts)
    tl.fill_start_end_places()

    for trip in tl.trips:
        try:
            trip_geojson = trip_to_geojson(trip, tl)
            geojson_list.append(trip_geojson)
        except Exception, e:
            logging.exception("Found error %s while processing trip %s" % (e, trip))
            raise e

    return geojson_list
