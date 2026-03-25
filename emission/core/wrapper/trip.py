from builtins import *
import logging
import emission.core.wrapper.wrapperbase as ecwb

# TODO: Figure out whether we should parse the geojson back to a geojson object

class Trip(ecwb.WrapperBase):
    props = {"start_ts": ecwb.WrapperBase.Access.WORM, # start UTC timestamp (in secs)
             "start_local_dt": ecwb.WrapperBase.Access.WORM, # start datetime in local time
             "start_fmt_time": ecwb.WrapperBase.Access.WORM, # start formatted time (in timezone of point)
             "end_ts": ecwb.WrapperBase.Access.WORM, # end UTC timestamp (in secs)
             "end_local_dt": ecwb.WrapperBase.Access.WORM, # end datetime in local time
             "end_fmt_time": ecwb.WrapperBase.Access.WORM, # end formatted time (in timezone of point)
             "start_place": ecwb.WrapperBase.Access.WORM,  # _id of place object before this one
             "end_place": ecwb.WrapperBase.Access.WORM,    # _id of place object after this one
             "start_loc": ecwb.WrapperBase.Access.WORM,    # location of start point in geojson format
             "end_loc": ecwb.WrapperBase.Access.WORM,      # location of end point in geojson format
             "duration": ecwb.WrapperBase.Access.WORM,     # duration of the trip in secs
             "distance": ecwb.WrapperBase.Access.WORM,     # distance of the trip in meters
             "source": ecwb.WrapperBase.Access.WORM}       # the method used to generate this trip

    enums = {}
    geojson = ["start_loc", "end_loc"]
    nullable = []
    local_dates = ['start_local_dt', 'end_local_dt']

    def _populateDependencies(self):
        pass
