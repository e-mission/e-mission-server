from __future__ import unicode_literals
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import
from future import standard_library
standard_library.install_aliases()
from builtins import *
import logging
import emission.core.wrapper.wrapperbase as ecwb

class Location(ecwb.WrapperBase):
    props = {"latitude": ecwb.WrapperBase.Access.RO,  # latitude of the point
             "longitude": ecwb.WrapperBase.Access.RO, # longitude of the point
             "loc": ecwb.WrapperBase.Access.RO,       # location of the point in geojson.
             "ts": ecwb.WrapperBase.Access.RO,        # timestamp (in seconds)
             "local_dt": ecwb.WrapperBase.Access.RO, # searchable datetime in local time
             "fmt_time": ecwb.WrapperBase.Access.RO,  # formatted time
             "altitude": ecwb.WrapperBase.Access.RO,  # altitude of the point
             "accuracy": ecwb.WrapperBase.Access.RO,  # horizontal accuracy of the point in meters.
        # This is the radius of the 68% confidence, so a lower
        # number means better accuracy
             "sensed_speed": ecwb.WrapperBase.Access.RO, # the speed reported by the phone in m/s
             "speed": ecwb.WrapperBase.Access.RO,      # the speed calculated by us
             "distance": ecwb.WrapperBase.Access.RO,   # distance calculated by us
             "heading": ecwb.WrapperBase.Access.RO,    # heading reported by the phone
             "vaccuracy": ecwb.WrapperBase.Access.RO,  # vertical accuracy of the point (only iOS)
             "floor": ecwb.WrapperBase.Access.RO}      # floor in a building that point is in (only iOS)
    enums = {}
    geojson = ["loc"]
    nullable = []
    local_dates = ['local_dt']

    def _populateDependencies(self):
        pass
