from __future__ import unicode_literals
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import
from future import standard_library
standard_library.install_aliases()
from builtins import *
from builtins import object
import logging
import emission.storage.timeseries.timequery as estt
import emission.storage.timeseries.tcquery as esttc
import emission.storage.timeseries.geoquery as estg

import emission.storage.decorations.analysis_timeseries_queries as esda
import emission.storage.timeseries.abstract_timeseries as esta

import emission.core.wrapper.entry as ecwe

def get_raw_timeline_from_dt(user_id, start_local_dt, end_local_dt,
                             geojson=None, extra_query_list=None):
    return get_timeline_from_dt(user_id, esda.RAW_PLACE_KEY, esda.RAW_TRIP_KEY, esda.RAW_UNTRACKED_KEY,
                                start_local_dt, end_local_dt, geojson, extra_query_list)

def get_cleaned_timeline_from_dt(user_id, start_local_dt, end_local_dt,
                                 geojson=None, extra_query_list=None):
    return get_timeline_from_dt(user_id, esda.CLEANED_PLACE_KEY, esda.CLEANED_TRIP_KEY, esda.CLEANED_UNTRACKED_KEY,
                                start_local_dt, end_local_dt, geojson, extra_query_list)

def get_confirmed_timeline_from_dt(user_id, start_local_dt, end_local_dt,
                                 geojson=None, extra_query_list=None):
    return get_timeline_from_dt(user_id, esda.CLEANED_PLACE_KEY, esda.CONFIRMED_TRIP_KEY, esda.CLEANED_UNTRACKED_KEY,
                                start_local_dt, end_local_dt, geojson, extra_query_list)

def get_raw_timeline(user_id, start_ts, end_ts,
                     geojson=None, extra_query_list=None):
    return get_timeline(user_id, esda.RAW_PLACE_KEY, esda.RAW_TRIP_KEY, esda.RAW_UNTRACKED_KEY,
                        start_ts, end_ts, geojson, extra_query_list)

def get_cleaned_timeline(user_id, start_ts, end_ts,
                         geojson=None, extra_query_list=None):
    return get_timeline(user_id, esda.CLEANED_PLACE_KEY, esda.CLEANED_TRIP_KEY, esda.CLEANED_UNTRACKED_KEY,
                        start_ts, end_ts, geojson, extra_query_list)

def get_confirmed_timeline(user_id, start_ts, end_ts,
                         geojson=None, extra_query_list=None):
    return get_timeline(user_id, esda.CLEANED_PLACE_KEY, esda.CONFIRMED_TRIP_KEY, esda.CLEANED_UNTRACKED_KEY,
                        start_ts, end_ts, geojson, extra_query_list)

def get_timeline(user_id, place_key, trip_key, untracked_key, start_ts, end_ts,
                 geojson=None, extra_query_list=None, trip_id_key=None):
    logging.info("About to query for timestamps %s -> %s" % (start_ts, end_ts))
    """
    Return a timeline of the trips and places from this start timestamp to this end timestamp.
    Note that each place and each trip has *two* associated timestamps, so we need to define which trips need to be
    returned. Right now, we define this as all places that are entered and all trips that are started within the
    specified time frame. Note that this means that, by definition, this may not include the starting and ending places
    for all trips, which is something that we need for our visualization. But we don't want the timeline to be
    visualization specific.
    Let's compromise by adding method to fill in start and end places which we will call if the timeline is used
    for visualization and not if not. This also means that we can use the id map to avoid duplicates in case the
    place does exist.
    :param user_id: the user whose timeline we are considering
    :param start_ts: the starting timestamp. we will include all places and trips that start after this.
    :param end_ts: the ending timestamp. we will include all places and trips that end after this.
    :return: a timeline object
    """
    (place_gq, trip_gq) = get_place_trip_geoquery(geojson)
    places_entries = esda.get_entries(place_key, user_id=user_id,
                                      time_query=estt.TimeQuery("data.enter_ts",
                                                                start_ts,
                                                                end_ts),
                                      geo_query=place_gq,
                                      extra_query_list=extra_query_list)
    trips_entries = esda.get_entries(trip_key, user_id=user_id,
                                     untracked_key=untracked_key,
                                     time_query=estt.TimeQuery("data.start_ts",
                                                               start_ts,
                                                               end_ts),
                                     geo_query=trip_gq,
                                     extra_query_list=extra_query_list)
    for place in places_entries:
        logging.debug("Considering place %s: %s -> %s " % (place.get_id(),
                        place.data.enter_fmt_time, place.data.exit_fmt_time))
    for trip in trips_entries:
        logging.debug("Considering trip %s: %s -> %s " % (trip.get_id(),
                        trip.data.start_fmt_time, trip.data.end_fmt_time))

    return Timeline(place_key, trip_key, places_entries, trips_entries, trip_id_key=trip_id_key)

def get_timeline_from_dt(user_id, place_key, trip_key, untracked_key,
                         start_local_dt, end_local_dt,
                         geojson=None, extra_query_list=None):
    logging.info("About to query for date components %s -> %s" % (start_local_dt, end_local_dt))
    (place_gq, trip_gq) = get_place_trip_geoquery(geojson)
    places_entries = esda.get_entries(place_key, user_id,
                                      esttc.TimeComponentQuery(
                                          "data.enter_local_dt", start_local_dt,
                                          end_local_dt),
                                      geo_query=place_gq,
                                      extra_query_list=extra_query_list)
    trips_entries = esda.get_entries(trip_key, user_id,
                                     untracked_key=untracked_key,
                                     time_query= esttc.TimeComponentQuery(
                                         "data.start_local_dt", start_local_dt,
                                         end_local_dt),
                                     geo_query=trip_gq,
                                     extra_query_list=extra_query_list)

    for place in places_entries:
        logging.debug("Considering place %s: %s -> %s " %
                      (place.get_id(), place.data.enter_fmt_time, place.data.exit_fmt_time))
    for trip in trips_entries:
        logging.debug("Considering trip %s: %s -> %s " %
                      (trip.get_id(), trip.data.start_fmt_time, trip.data.end_fmt_time))

    return Timeline(place_key, trip_key, places_entries, trips_entries)

def get_place_trip_geoquery(geojson):
    if geojson is not None:
        place_gq = estg.GeoQuery(loc_field_list=['data.location'],
                                 poly_region=geojson)
        trip_gq = estg.GeoQuery(loc_field_list=['data.start_loc', 'data.end_loc'],
                                poly_region=geojson)
    else:
        place_gq = None
        trip_gq = None
    return (place_gq, trip_gq)

class Timeline(object):

    class State(object):
        def __init__(self, type, element):
            self.element_type = type
            self.element = element
            if element is None:
                self.id = None
            else:
                self.id = element.get_id()


    def __init__(self, place_or_stop_key, trip_or_section_key,
                 places_or_stops_entries, trips_or_sections_entries, trip_id_key=None):
        """
        :param trip_id_key: We may want to identify trips by a different key than their '_id'.
                            For example, if we want a timeseries of CLEANED_PLACE and EXPECTED_TRIP,
                            we will need to identify the expected trips by their 'cleaned_trip' key
                            because this is what the cleaned places refer to.
                            If this is None, we just use the usual '_id'.
        """
        logging.debug("keys = (%s, %s), len(places) = %s, len(trips) = %s" %
                      (place_or_stop_key, trip_or_section_key,
                       len(places_or_stops_entries), len(trips_or_sections_entries)))
        logging.debug("places_or_stops = %s " %
                        [p.get_id() for p in places_or_stops_entries])
        logging.debug("trips_or_sections = %s " %
                      [s.get_id() for s in trips_or_sections_entries])

        self.place_key = place_or_stop_key
        self.trip_key = trip_or_section_key
        self.places = places_or_stops_entries
        self.trips = trips_or_sections_entries
        self.id_map = dict((p.get_id(), p) for p in self.places)
        if trip_id_key:
            trip_ids = [(t['data'][trip_id_key], t) for t in self.trips if trip_id_key in t['data']]
            self.id_map.update(dict(trip_ids))
        else:
            self.id_map.update(dict((t.get_id(), t) for t in self.trips))

        if (len(self.places) == 0) and (len(self.trips) == 0):
            self.state = Timeline.State("none", None)
        elif len(self.places) == 0:
                self.state = Timeline.State("trip", self.trips[0])
        elif len(self.trips) == 0:
                self.state = Timeline.State("place", self.places[0])
        else:
            assert (len(self.places) > 0 and len(self.trips) > 0)
            logging.debug("to find start, comparing place %s with trip %s" %
                          (self.places[0].get_id(), self.trips[0].get_id()))
            if self.places[0].data.exit_ts is not None and self.places[0].data.exit_ts <= self.trips[0].data.start_ts:
                self.state = Timeline.State("place", self.places[0])
            else:
                self.state = Timeline.State("trip", self.trips[0])
        logging.debug("Starting with element of type %s, id %s, details %s" %
                      (self.state.element_type, self.state.id, self.state.element))

    def fill_start_end_places(self):
        """
        Must be called before we start iterating over the values. If this is called in the middle of the iteration,
        the results are undefined. In particular, they will be different depending on whether it was called when we
        were iterating over a place versus a trip.
        :return: None. The timeline is updated with the start and end place.
        """
        for trip in self.trips:
            logging.debug("for trip = %s, adding start_place %s and end_place %s" % (trip.get_id(),
                                                                                    trip.data.start_place,
                                                                                    trip.data.end_place))
            start_place = self._addIfNotExists(trip.data.start_place)
            end_place = self._addIfNotExists(trip.data.end_place)
            self.state = Timeline.State("place", start_place)  # Since this has been called before the iteration start

    def get_object(self, element_id):
        """
        Return the object corresponding to the id from the in-memory map. This should be more efficient than
        going to the database.
        :param element_id: the id for which we want to retrieve the element
        :return: the matching object
        """
        return self.id_map[element_id]

    def _addIfNotExists(self, place_id):
        """
        Adds the place specified by the given place_id to the place list and the place map and returns it
        :param place_id:
        :return:
        """
        import emission.storage.decorations.place_queries as esdp

        if place_id not in self.id_map:
            logging.debug("place id %s is not in the map, searching in database" % place_id)
            place_entry = esda.get_entry(self.place_key, place_id)
            self.places.append(place_entry)
            self.id_map[place_id] = place_entry
            logging.debug("retrieved object %s and added to id_map" % place_entry)
            return place_entry
        else:
            return self.id_map[place_id]


    def __iter__(self):
        return self

    def __next__(self):
        ret_val = self.state.element
        # logging.debug("ret_val = %s" % ret_val)
        if ret_val is None:
            # logging.info("ret_val is None, raising StopIteration")
            raise StopIteration
        # This is thread safe since there is no defined way to mutate the
        # curr_element outside of move_to_next(), adn move_to_next() is
        # synchronous, so this call will complete before it returns.
        self.move_to_next()
        return ret_val

    def move_to_next(self):
        # TODO: restructure the data model to avoid this stupid if/then.
        # e.g. rename field to be "starting" for both trip and section
        if self.state.element_type == "place":
            if hasattr(self.state.element.data, "starting_trip"):
                new_id = self.state.element.data.starting_trip
            else:
                assert(hasattr(self.state.element.data, "starting_section"))
                new_id = self.state.element.data.starting_section
            new_type = "trip"
        else:
            assert(self.state.element_type == "trip")
            if hasattr(self.state.element.data, "end_place"):
                new_id = self.state.element.data.end_place
            else:
                assert(hasattr(self.state.element.data, "end_stop"))
                new_id = self.state.element.data.end_stop
            new_type = "place"

        if new_id is None:
            self.state = Timeline.State("unknown", None)
        else:
            self.state = Timeline.State(new_type, self.id_map[new_id])

    def is_empty(self):
        return len(self.trips) == 0 and len(self.places) == 0

    def first_place(self):
        if self.first_trip() is not None and \
            self.first_trip().data.start_place is not None:
            return self.get_object(self.first_trip().data.start_place)
        else:
            return None

    def last_place(self):
        if self.last_trip() is not None and \
            self.last_trip().data.end_place is not None:
            return self.get_object(self.last_trip().data.end_place)
        else:
            return None

    def first_trip(self):
        return self.trips[0] if len(self.trips) > 0 else None

    def last_trip(self):
        return self.trips[-1] if len(self.trips) > 0 else None
