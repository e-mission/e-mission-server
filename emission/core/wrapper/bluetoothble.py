import logging
import enum as enum
import emission.core.wrapper.wrapperbase as ecwb

class BLEEventTypes(enum.Enum):
    REGION_ENTER = 0
    REGION_EXIT = 1
    RANGE_UPDATE = 2

class Bluetoothble(ecwb.WrapperBase):
    props = {"eventType": ecwb.WrapperBase.Access.RO, # the type of event
             "uuid": ecwb.WrapperBase.Access.RO,  # UUID of the beacon. Will be a constant for beacons used by e-mission, consistent with https://github.com/e-mission/e-mission-docs/issues/1062#issuecomment-2026359038
             "major": ecwb.WrapperBase.Access.RO, # major value (matches deployment)
             "minor": ecwb.WrapperBase.Access.RO,       # minor value (matches the actual vehicle)
             "ts": ecwb.WrapperBase.Access.RO,        # timestamp (in seconds)
             "proximity": ecwb.WrapperBase.Access.RO,  # how close the beacon is (used as the second step in the process, https://github.com/e-mission/e-mission-docs/issues/1062#issuecomment-2026359038
             "local_dt": ecwb.WrapperBase.Access.RO, # searchable datetime in local time
             "fmt_time": ecwb.WrapperBase.Access.RO,  # formatted time
             "accuracy": ecwb.WrapperBase.Access.RO,  # only available for range updats
             "rssi": ecwb.WrapperBase.Access.RO, # signal strength, only available for range updates
    }

    enums = {"eventType": BLEEventTypes}
    geojson = []
    nullable = ["major", "minor", "proximity", "accuracy", "rssi"]
    local_dates = ['local_dt']

    def _populateDependencies(self):
        pass
