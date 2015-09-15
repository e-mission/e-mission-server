import logging
import emission.core.wrapper.wrapperbase as ecwb

class Location(ecwb.WrapperBase):
    props = {"latitude": ecwb.WrapperBase.Access.RO,  # latitude of the point
             "longitude": ecwb.WrapperBase.Access.RO, # longitude of the point
             "ts": ecwb.WrapperBase.Access.RO,        # timestamp (in seconds)
             "fmt_time": ecwb.WrapperBase.Access.RO,  # formatted time
             "altitude": ecwb.WrapperBase.Access.RO,  # altitude of the point
             "accuracy": ecwb.WrapperBase.Access.RO,  # horizontal accuracy of the point in meters.
        # This is the radius of the 68% confidence, so a lower
        # number means better accuracy
             "sensed_speed": ecwb.WrapperBase.Access.RO, # the speed reported by the phone
             "speed": ecwb.WrapperBase.Access.RO,      # the speed calculated by us
             "distance": ecwb.WrapperBase.Access.RO,   # distance calculated by us
             "heading": ecwb.WrapperBase.Access.RO,    # heading reported by the phone
             "vaccuracy": ecwb.WrapperBase.Access.RO,  # vertical accuracy of the point (only iOS)
             "floor": ecwb.WrapperBase.Access.RO}      # floor in a building that point is in (only iOS)
    enums = {}

    def _populateDependencies(self):
        pass
