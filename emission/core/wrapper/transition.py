import logging
import emission.core.wrapper.wrapperbase as ecwb
import enum as enum

class State(enum.Enum):
    START = 0
    WAITING_FOR_TRIP_START = 1
    ONGOING_TRIP = 2

class TransitionType(enum.Enum):
    INITIALIZE = 0
    EXITED_GEOFENCE = 1
    STOPPED_MOVING = 2
    STOP_TRACKING = 3

class Transition(ecwb.WrapperBase):
    props = {"curr_state": ecwb.WrapperBase.Access.RO,
             "transition": ecwb.WrapperBase.Access.RO,
             "ts": ecwb.WrapperBase.Access.RO,
             "fmt_time": ecwb.WrapperBase.Access.RO
            }

    enums = {"curr_state": State, "transition": TransitionType}

    def _populateDependencies(self):
        pass
