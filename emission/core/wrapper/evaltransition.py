import logging
import emission.core.wrapper.wrapperbase as ecwb
import enum as enum

class TransitionType(enum.Enum):
    START_CALIBRATION_PERIOD = 0
    STOP_CALIBRATION_PERIOD = 1
    START_EVALUATION_PERIOD = 2
    STOP_EVALUATION_PERIOD = 3
    START_EVALUATION_TRIP = 4
    STOP_EVALUATION_TRIP = 5
    ENTER_EVALUATION_STOP = 6
    LEAVE_EVALUATION_STOP = 7

class Transition(ecwb.WrapperBase):
    props = {"transition": ecwb.WrapperBase.Access.RO,
             "trip_id": ecwb.WrapperBase.Access.RO,
             "spec_id": ecwb.WrapperBase.Access.RO,
             "device_manufacturer": ecwb.WrapperBase.Access.RO,
             "device_model": ecwb.WrapperBase.Access.RO,
             "device_version": ecwb.WrapperBase.Access.RO,
             "ts": ecwb.WrapperBase.Access.RO,
             "local_dt": ecwb.WrapperBase.Access.RO,
             "fmt_time": ecwb.WrapperBase.Access.RO
            }

    enums = {"transition": TransitionType}
    geojson = []
    nullable = []
    local_dates = ['local_dt']

    def _populateDependencies(self):
        pass
