import logging
import emission.core.wrapper.wrapperbase as ecwb
import enum as enum

class State(enum.Enum):
    START = 0
    WAITING_FOR_TRIP_START = 1
    ONGOING_TRIP = 2
    TRACKING_STOPPED = 3
    UNKNOWN = 3

class TransitionType(enum.Enum):
    INITIALIZE = 0
    EXITED_GEOFENCE = 1
    STOPPED_MOVING = 2
    STOP_TRACKING = 3
    # android specific transitions
    BOOTED = 4
    TRACKING_ERROR = 18
    # iOS specific transitions
    INIT_COMPLETE = 5
    TRIP_STARTED = 6
    RECEIVED_SILENT_PUSH = 7
    TRIP_END_DETECTED = 8
    TRIP_RESTARTED = 9
    END_TRIP_TRACKING = 10
    TRACKING_STOPPED = 11
    NOP = 12
    VISIT_STARTED = 13
    VISIT_ENDED = 14
    NONE = 15
    DATA_PUSHED = 16
    # joint transition again
    START_TRACKING = 17

class Transition(ecwb.WrapperBase):
    props = {"curr_state": ecwb.WrapperBase.Access.RO,
             "transition": ecwb.WrapperBase.Access.RO,
             "ts": ecwb.WrapperBase.Access.RO,
             "local_dt": ecwb.WrapperBase.Access.RO,
             "fmt_time": ecwb.WrapperBase.Access.RO
            }

    enums = {"curr_state": State, "transition": TransitionType}
    geojson = []
    nullable = []
    local_dates = ['local_dt']

    def _populateDependencies(self):
        pass
