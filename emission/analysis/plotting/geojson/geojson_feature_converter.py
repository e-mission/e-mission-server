from __future__ import print_function
from __future__ import unicode_literals
from __future__ import division
from __future__ import absolute_import
from future import standard_library
standard_library.install_aliases()
from builtins import map
from builtins import str
from builtins import *
import logging
import geojson as gj
import copy
import attrdict as ad
import pandas as pd

import emission.storage.timeseries.abstract_timeseries as esta
import emission.net.usercache.abstract_usercache as enua
import emission.storage.timeseries.timequery as estt

import emission.storage.decorations.trip_queries as esdt
import emission.storage.decorations.analysis_timeseries_queries as esda
import emission.storage.decorations.section_queries as esds
import emission.storage.decorations.timeline as esdtl

import emission.core.wrapper.location as ecwl
import emission.core.wrapper.cleanedsection as ecwcs
import emission.core.wrapper.entry as ecwe
import emission.core.common as ecc

# TODO: Move this to the section_features class instead
import emission.analysis.intake.cleaning.location_smoothing as eaicl
import emission.analysis.config as eac

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
    except Exception as e:
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

        if not ecc.compare_rounded_arrays(section.data.end_loc.coordinates,
                                      section_location_entries[-1].data.loc.coordinates,
                                      digits=4):
            logging.info("section_location_array[-1].data.loc %s != section.data.end_loc %s even after df.ts fix, filling gap" % \
                    (section_location_entries[-1].data.loc, section.data.end_loc))
            if eac.get_config()["output.conversion.validityAssertions"]:
                assert(False)
            last_loc_doc = ts.get_entry_at_ts("background/filtered_location", "data.ts", section.data.end_ts)
            if last_loc_doc is None:
                logging.warning("can't find entry to patch gap, leaving gap")
            else:
                last_loc_entry = ecwe.Entry(last_loc_doc)
                logging.debug("Adding new entry %s to fill the end point gap between %s and %s"
                   % (last_loc_entry.data.loc, section_location_entries[-1].data.loc,
                        section.data.end_loc))
                section_location_entries.append(last_loc_entry)

    points_line_feature = point_array_to_line(section_location_entries)
    points_line_feature.id = str(section.get_id())
    points_line_feature.properties.update(copy.copy(section.data))
    # Update works on dicts, convert back to a section object to make the modes
    # work properly
    points_line_feature.properties = ecwcs.Cleanedsection(points_line_feature.properties)

    points_line_feature.properties["feature_type"] = "section"

    if eac.get_section_key_for_analysis_results() == esda.INFERRED_SECTION_KEY:
        ise = esds.cleaned2inferred_section(section.user_id, section.get_id())
        if ise is not None:
            logging.debug("mapped cleaned section %s -> inferred section %s" % 
                (section.get_id(), ise.get_id()))
            logging.debug("changing mode from %s -> %s" % 
                (points_line_feature.properties.sensed_mode, ise.data.sensed_mode))
            points_line_feature.properties["sensed_mode"] = str(ise.data.sensed_mode)
        else:
            points_line_feature.properties["sensed_mode"] = str(points_line_feature.properties.sensed_mode)
    else:
        points_line_feature.properties["sensed_mode"] = str(points_line_feature.properties.sensed_mode)
    
    _del_non_derializable(points_line_feature.properties, ["start_loc", "end_loc"])

    # feature_array.append(gj.FeatureCollection(points_feature_array))
    feature_array.append(points_line_feature)

    return gj.FeatureCollection(feature_array)

def incident_to_geojson(incident):
    ret_feature = gj.Feature()
    ret_feature.id = str(incident.get_id())
    ret_feature.geometry = gj.Point()
    ret_feature.geometry.coordinates = incident.data.loc.coordinates
    ret_feature.properties = copy.copy(incident.data)
    ret_feature.properties["feature_type"] = "incident"

    # _stringify_foreign_key(ret_feature.properties, ["ending_section", "starting_section", "trip_id"])
    _del_non_derializable(ret_feature.properties, ["loc"])
    return ret_feature

def geojson_incidents_in_range(user_id, start_ts, end_ts):
    MANUAL_INCIDENT_KEY = "manual/incident"
    ts = esta.TimeSeries.get_time_series(user_id)
    uc = enua.UserCache.getUserCache(user_id)
    tq = estt.TimeQuery("data.ts", start_ts, end_ts)
    incident_entry_docs = list(ts.find_entries([MANUAL_INCIDENT_KEY], time_query=tq)) \
        + list(uc.getMessage([MANUAL_INCIDENT_KEY], tq))
    incidents = [ecwe.Entry(doc) for doc in incident_entry_docs]
    return list(map(incident_to_geojson, incidents))

def point_array_to_line(point_array):
    points_line_string = gj.LineString()
    # points_line_string.coordinates = [l.loc.coordinates for l in filtered_section_location_array]
    points_line_string.coordinates = []
    points_times = []

    for l in point_array:
        # logging.debug("About to add %s to line_string " % l)
        points_line_string.coordinates.append(l.data.loc.coordinates)
        points_times.append(l.data.ts)
    
    points_line_feature = gj.Feature()
    points_line_feature.geometry = points_line_string
    points_line_feature.properties = {}
    points_line_feature.properties["times"] = points_times
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
        section_gj = section_to_geojson(section, tl)
        feature_array.append(section_gj)

    trip_geojson = gj.FeatureCollection(features=feature_array, properties=trip.data)
    trip_geojson.id = str(trip.get_id())

    feature_array.extend(geojson_incidents_in_range(trip.user_id,
                                              curr_start_place.data.exit_ts,
                                              curr_end_place.data.enter_ts))
    if trip.metadata.key == esda.CLEANED_UNTRACKED_KEY:
        # trip_geojson.properties["feature_type"] = "untracked"
        # Since the "untracked" type is not correctly handled on the phone, we just
        # skip these trips until
        # https://github.com/e-mission/e-mission-phone/issues/118
        # is fixed
        # TODO: Once it is fixed, re-introduce the first line in this block
        # and remove the None check in get_geojson_for_timeline
        return None
    else:
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
            if trip_geojson is not None:
                geojson_list.append(trip_geojson)
        except Exception as e:
            logging.exception("Found error %s while processing trip %s" % (e, trip))
            raise e
    logging.debug("trip count = %d, geojson count = %d" %
                  (len(tl.trips), len(geojson_list)))
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
    points_feature_array = [location_to_geojson(le) for le in points_array]
    print ("Found %d features from %d points" %
           (len(points_feature_array), len(points_array)))
    
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
            "dummy_user", "background/location", ecwl.Location(retVal)))
    return location_entry_list
