import logging
import emission.net.usercache.abstract_usercache as enua

def get_timeline(user_id, start_ts, end_ts):
    # These imports are in here to avoid circular import dependencies between
    # trip_queries and this file (timeline)
    import emission.storage.decorations.place_queries as esdp
    import emission.storage.decorations.trip_queries as esdt

    places = esdp.get_places(user_id, enua.UserCache.TimeQuery("enter_ts", start_ts, end_ts))
    trips = esdt.get_trips(user_id, enua.UserCache.TimeQuery("start_ts", start_ts, end_ts))
    return Timeline(places, trips)

class Timeline(object):

    class State(object):
        def __init__(self, type, element):
            self.type = type
            self.element = element
            if element is None:
                self.id = None
            else:
                self.id = element.get_id()


    def __init__(self, places_or_stops, trips_or_sections):
        logging.debug("len(places) = %s, len(trips) = %s" % (len(places_or_stops), len(trips_or_sections)))
        self.places = places_or_stops
        self.trips = trips_or_sections
        self.id_map = dict((p.get_id(), p) for p in self.places)
        self.id_map.update(dict((t.get_id(), t) for t in self.trips))

        if (len(self.places) == 0) and (len(self.trips) == 0):
            self.state = Timeline.State("none", None)
        elif len(self.places) == 0:
                self.state = Timeline.State("trip", self.trips[0])
        elif len(self.trips) == 0:
                self.state = Timeline.State("place", self.places[0])
        else:
            assert (len(self.places) > 0 and len(self.trips) > 0)
            if self.places[0].exit_ts < self.trips[0].start_ts:
                self.state = Timeline.State("place", self.places[0])
            else:
                self.state = Timeline.State("trip", self.trips[0])
        logging.debug("Starting with element of type %s, id %s, details %s" % (self.state.type, self.state.id, self.state.element))

    def __iter__(self):
        return self

    def next(self):
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
        if self.state.type == "place":
            if hasattr(self.state.element, "starting_trip"):
                new_id = self.state.element.starting_trip
            else:
                assert(hasattr(self.state.element, "starting_section"))
                new_id = self.state.element.starting_section
            new_type = "trip"
        else:
            assert(self.state.type == "trip")
            if hasattr(self.state.element, "end_place"):
                new_id = self.state.element.end_place
            else:
                assert(hasattr(self.state.element, "end_stop"))
                new_id = self.state.element.end_stop
            new_type = "place"

        if new_id is None:
            self.state = Timeline.State("unknown", None)
        else:
            self.state = Timeline.State(new_type, self.id_map[new_id])
