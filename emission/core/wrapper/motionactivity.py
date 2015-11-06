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

    def _populateDependencies(self):
        pass
