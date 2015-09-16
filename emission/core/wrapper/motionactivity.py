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

class Motionactivity(ecwb.WrapperBase):
    props = {"type": ecwb.WrapperBase.Access.RO,
             "confidence": ecwb.WrapperBase.Access.RO,
             "ts": ecwb.WrapperBase.Access.RO,
             "fmt_time": ecwb.WrapperBase.Access.RO
            }

    enums = {"type": MotionTypes}
    geojson = []

    def _populateDependencies(self):
        pass
