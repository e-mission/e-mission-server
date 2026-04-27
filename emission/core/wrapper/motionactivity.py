from builtins import *
import logging
import emission.core.wrapper.wrapperbase as ecwb
import enum as enum

class MotionTypes(enum.Enum):
    IN_VEHICLE = 0
    BICYCLING = 1
    ON_FOOT = 2
    STILL = 3
    UNKNOWN = 4
    TILTING = 5 
    WALKING = 7
    RUNNING = 8
    # iOS only. Sometimes, the activity has all modes = false.
    # I don't know how/why that should happen, but if it does,
    # What we probably want to do is to ignore such entries,
    # but we can't really do so while copying because every input
    # in the user cache has an entry in long-term. So we create
    # a "none" type to support it, and will ignore it when we 
    # do the segmentation
    NONE = 9
    # iOS only. iOS supports multiple activities being true simultaneously.
    # From the CMMotionActivity documentation,
    # "The motion-related properties of this class are not mutually exclusive.
    # In other words, it is possible for more than one of the motion-related
    # properties to contain the value YES. For example, if the user was driving
    # in a car and the car stopped at a red light, the update event associated 
    # with that change in motion would have both the automotive and stationary
    # properties set to YES."
    # In our examples also, this is the only case that I have seen with 
    # multiple values = true. In general, we should probably handle this with a 
    # bit string, but that will also make the segmentation logic
    # more complex. Let us just create a type for this combination and move on
    # for now. If we see other combinations in the future, we can figure out 
    # how to deal with it at that time.
    STOPPED_WHILE_IN_VEHICLE = 10
    # Detected during the clean and resample stage. NOT detected from the
    # phone. Initial version of more general mode inference
    AIR_OR_HSR = 11

class Motionactivity(ecwb.WrapperBase):
    props = {"type": ecwb.WrapperBase.Access.RO,
             "confidence": ecwb.WrapperBase.Access.RO,
             "ts": ecwb.WrapperBase.Access.RO,
             "local_dt": ecwb.WrapperBase.Access.RO,
             "fmt_time": ecwb.WrapperBase.Access.RO
            }

    enums = {"type": MotionTypes}
    geojson = []
    nullable = []
    local_dates = ['local_dt']

    def _populateDependencies(self):
        pass
