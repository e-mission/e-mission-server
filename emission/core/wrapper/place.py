import logging
import emission.core.wrapper.wrapperbase as ecwb

class Place(ecwb.WrapperBase):
    props = {"user_id": ecwb.WrapperBase.Access.WORM, # start UTC timestamp (in secs)
             "enter_ts": ecwb.WrapperBase.Access.WORM,  # the timestamp of entry (in secs)
             "enter_fmt_time": ecwb.WrapperBase.Access.WORM, # formatted entry time in timezone of place
             "exit_ts": ecwb.WrapperBase.Access.WORM,        # the timestamp of exit (in secs)
             "exit_fmt_time": ecwb.WrapperBase.Access.WORM,  # formatted time in timezone of place
             "ending_trip": ecwb.WrapperBase.Access.WORM,  # the id of the trip just before this
             "starting_trip": ecwb.WrapperBase.Access.WORM,  # the id of the trip just after this
             "location": ecwb.WrapperBase.Access.WORM, # the location in geojson format
             "source": ecwb.WrapperBase.Access.WORM,   # the method used to generate this place
             "duration": ecwb.WrapperBase.Access.WORM}    # the duration for which we were in this place

    enums = {}
    geojson = ["location"]
    nullable = ["enter_ts", "enter_fmt_time", "ending_trip", # for the start of a chain
                "exit_ts", "exit_fmt_time", "starting_trip"] # for the end of a chain

    def _populateDependencies(self):
        pass

    def __getattr__(self, key):
        # TODO: If this is a recurring pattern, might want to support a "nullable" list as well
        if (key in self.nullable) and (key not in self):
            return None
        else:
            return super(Place, self).__getattr__(key)
