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

    def _populateDependencies(self):
        pass
