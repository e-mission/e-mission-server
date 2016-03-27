import logging
import emission.core.wrapper.wrapperbase as ecwb
import enum as enum

class BatteryStatus(enum.Enum):
    UNKNOWN = 0
    DISCHARGING = 1
    CHARGING = 2
    FULL = 3
    NOT_CHARGING = 4 # This is an android-only state - unsure how often we will encounter it

class Battery(ecwb.WrapperBase):
    props = {"battery_level_pct": ecwb.WrapperBase.Access.RO,  # percentage of the battery left. value between 0 and 100
             "battery_status": ecwb.WrapperBase.Access.RO, # Current status - charging, discharging or full
             "android_health": ecwb.WrapperBase.Access.RO, # android-only battery health indicator
             "android_level": ecwb.WrapperBase.Access.RO, # android-only level indicator (compared to scale)
             "android_scale": ecwb.WrapperBase.Access.RO, # scale to be used with android_level
             "android_plugged": ecwb.WrapperBase.Access.RO,     # source that it is plugged into
             "android_technology": ecwb.WrapperBase.Access.RO, # technology used to make the battery
             "android_temperature": ecwb.WrapperBase.Access.RO,  # android-only: current temperature
             "android_voltage": ecwb.WrapperBase.Access.RO,  # android-only: current voltage
             "android_current_average": ecwb.WrapperBase.Access.RO,  # average current
             "android_current_instant": ecwb.WrapperBase.Access.RO, # instantaneous current
             "android_energy_counter": ecwb.WrapperBase.Access.RO, # remaining energy in nanowatt hours
             "ts": ecwb.WrapperBase.Access.RO,
             "local_dt": ecwb.WrapperBase.Access.RO,
             "fmt_time": ecwb.WrapperBase.Access.RO
            }
    enums = {"battery_status": BatteryStatus}
    geojson = []
    nullable = []

    def _populateDependencies(self):
        pass
