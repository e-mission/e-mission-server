import logging
import geojson as gj
import copy
import attrdict as ad
import pandas as pd

import emission.storage.timeseries.abstract_timeseries as esta

import emission.storage.decorations.trip_queries as esdt
import emission.storage.decorations.analysis_timeseries_queries as esda
import emission.storage.decorations.timeline as esdtl

import emission.core.wrapper.location as ecwl
import emission.core.wrapper.entry as ecwe

# TODO: Move this to the section_features class instead
import emission.analysis.intake.cleaning.location_smoothing as eaicl

def _del_non_derializable(prop_dict, extra_keys):
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
        ret_feature.geometry = location.data.loc
        ret_feature.properties = copy.copy(location.data)
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
    ret_feature.geometry = place.data.location
    ret_feature.properties = copy.copy(place.data)
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
    ret_feature.geometry.coordinates = [stop.data.enter_loc.coordinates, stop.data.exit_loc.coordinates]
    ret_feature.properties = copy.copy(stop.data)
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
    entry_it = ts.find_entries(["analysis/recreated_location"],
                               esda.get_time_query_for_trip_like(
                                   "analysis/cleaned_section",
                                   section.get_id()))

    # TODO: Decide whether we want to use Rewrite to use dataframes throughout instead of python arrays.
    # dataframes insert nans. We could use fillna to fill with default values, but if we are not actually
    # using dataframe features here, it is unclear how much that would help.
    feature_array = []
    section_location_entries = [ecwe.Entry(entry) for entry in entry_it]
    if len(section_location_entries) != 0:
        logging.debug("first element in section_location_array = %s" % section_location_entries[0])

        # Fudge the end point so that we don't have a gap because of the ts != write_ts mismatch
        # TODO: Fix this once we are able to query by the data timestamp instead of the metadata ts

        assert section_location_entries[-1].data.loc == section.data.end_loc, \
                "section_location_array[-1].data.loc %s != section.data.end_loc %s even after df.ts fix" % \
                    (section_location_entries[-1].data.loc, section.data.end_loc)
#             last_loc_doc = ts.get_entry_at_ts("background/filtered_location", "data.ts", section.end_ts)
#             last_loc_data = ecwe.Entry(last_loc_doc).data
#             last_loc_data["_id"] = last_loc_doc["_id"]
#             section_location_array.append(last_loc_data)
#             logging.debug("Adding new entry %s to fill the end point gap between %s and %s"
#                 % (last_loc_data.loc, section_location_array[-2].loc, section.end_loc))

    # points_feature_array = [location_to_geojson(l) for l in filtered_section_location_array]

    points_line_feature = point_array_to_line(section_location_entries)
    # If this is the first section, we already start from the trip start. But we actually need to start from the
    # prior place. Fudge this too. Note also that we may want to figure out how to handle this properly in the model
    # without needing fudging. TODO: Unclear how exactly to do this
    if section.data.start_stop is None:
        # This is the first section. So we need to find the start place of the parent trip
        parent_trip = tl.get_object(section.data.trip_id)
        start_place_of_parent_trip = tl.get_object(parent_trip.data.start_place)
        points_line_feature.geometry.coordinates.insert(0, start_place_of_parent_trip.data.location.coordinates)

    points_line_feature.id = str(section.get_id())
    points_line_feature.properties = copy.copy(section.data)
    points_line_feature.properties["feature_type"] = "section"
    points_line_feature.properties["sensed_mode"] = str(points_line_feature.properties.sensed_mode)

    _del_non_derializable(points_line_feature.properties, ["start_loc", "end_loc"])

    # feature_array.append(gj.FeatureCollection(points_feature_array))
    feature_array.append(points_line_feature)

    return gj.FeatureCollection(feature_array)

def point_array_to_line(point_array):
    points_line_string = gj.LineString()
    # points_line_string.coordinates = [l.loc.coordinates for l in filtered_section_location_array]
    points_line_string.coordinates = []

    for l in point_array:
        # logging.debug("About to add %s to line_string " % l)
        points_line_string.coordinates.append(l.data.loc.coordinates)
    
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
    curr_start_place = tl.get_object(trip.data.start_place)
    curr_end_place = tl.get_object(trip.data.end_place)
    start_place_geojson = place_to_geojson(curr_start_place)
    start_place_geojson["properties"]["feature_type"] = "start_place"
    feature_array.append(start_place_geojson)

    end_place_geojson = place_to_geojson(curr_end_place)
    end_place_geojson["properties"]["feature_type"] = "end_place"
    feature_array.append(end_place_geojson)

    trip_tl = esdt.get_cleaned_timeline_for_trip(trip.user_id, trip.get_id())
    stops = trip_tl.places
    for stop in stops:
        feature_array.append(stop_to_geojson(stop))

    for i, section in enumerate(trip_tl.trips):
        # TODO: figure out whether we should do this at the model.
        # The first section starts with the start of the trip. But the trip itself starts at the first
        # point where we exit the geofence, not at the start place. That is because we don't really know when
        # we left the start place. We can fix this in the model through interpolation. For now, we assume that the
        # gap between the real departure time and the time that the trip starts is small, and just combine it here.
        section_gj = section_to_geojson(section, tl)
        feature_array.append(section_gj)

    trip_geojson = gj.FeatureCollection(features=feature_array, properties=trip.data)
    trip_geojson.id = str(trip.get_id())
    trip_geojson.properties["feature_type"] = "trip"
    return trip_geojson

def get_geojson_for_ts(user_id, start_ts, end_ts):
    tl = esdtl.get_cleaned_timeline(user_id, start_ts, end_ts)
    tl.fill_start_end_places()
    return get_geojson_for_timeline(user_id, tl)

def get_geojson_for_dt(user_id, start_local_dt, end_local_dt):
    logging.debug("Getting geojson for %s -> %s" % (start_local_dt, end_local_dt))
    tl = esdtl.get_cleaned_timeline_from_dt(user_id, start_local_dt, end_local_dt)
    tl.fill_start_end_places()
    return get_geojson_for_timeline(user_id, tl)

def get_geojson_for_timeline(user_id, tl):
    """
    tl represents the "timeline" object that is queried for the trips and locations
    """
    geojson_list = []

    for trip in tl.trips:
        try:
            trip_geojson = trip_to_geojson(trip, tl)
            geojson_list.append(trip_geojson)
        except Exception, e:
            logging.exception("Found error %s while processing trip %s" % (e, trip))
            raise e
    return geojson_list

def get_all_points_for_range(user_id, key, start_ts, end_ts):
    import emission.storage.timeseries.timequery as estt
#     import emission.core.wrapper.location as ecwl 
    
    tq = estt.TimeQuery("metadata.write_ts", start_ts, end_ts)
    ts = esta.TimeSeries.get_time_series(user_id)
    entry_it = ts.find_entries([key], tq)
    points_array = [ecwe.Entry(entry) for entry in entry_it]

    return get_feature_list_for_point_array(points_array)


def get_feature_list_for_point_array(points_array):
    points_feature_array = [location_to_geojson(le.data) for le in points_array]
    print ("Found %d points" % len(points_feature_array))
    
    feature_array = []
    feature_array.append(gj.FeatureCollection(points_feature_array))
    feature_array.append(point_array_to_line(points_array))
    feature_coll = gj.FeatureCollection(feature_array)
        
    return feature_coll

def get_feature_list_from_df(loc_time_df, ts="ts", latitude="latitude", longitude="longitude", fmt_time="fmt_time"):
    """
    Input DF should have columns called "ts", "latitude" and "longitude", or the corresponding
    columns can be passed in using the ts, latitude and longitude parameters
    """
    points_array = get_location_entry_list_from_df(loc_time_df, ts, latitude, longitude, fmt_time)
    return get_feature_list_for_point_array(points_array)

def get_location_entry_list_from_df(loc_time_df, ts="ts", latitude="latitude", longitude="longitude", fmt_time="fmt_time"):
    location_entry_list = []
    for idx, row in loc_time_df.iterrows():
        retVal = {"latitude": row[latitude], "longitude": row[longitude], "ts": row[ts],
                  "_id": str(idx), "fmt_time": row[fmt_time], "loc": gj.Point(coordinates=[row[longitude], row[latitude]])}
        location_entry_list.append(ecwe.Entry.create_entry(
            "dummy_user", "dummy_entry", ecwl.Location(retVal)))
    return location_entry_list
