from __future__ import unicode_literals
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import
from future import standard_library
standard_library.install_aliases()
from builtins import *
import logging
import emission.core.wrapper.wrapperbase as ecwb
import enum as enum

# class StatsEvent(enum.Enum):
#     """
#     Indicates that some event happened that we want to record.
#     The event can have an associated duration, or can be a
#     """
#     UNKNOWN = 0
#     DISCHARGING = 1
#     CHARGING = 2
#     FULL = 3
#     NOT_CHARGING = 4 # This is an android-only state - unsure how often we will encounter it
#

class Statsevent(ecwb.WrapperBase):
    # TODO: should this be a string or an enum
    # making it an enum will require us to change code every time we add a new stat, but will
    # make it easier to know the list of stats. Let's leave it as a string for now.
    props = {"name": ecwb.WrapperBase.Access.WORM,  # string representing the stat.
             "reading": ecwb.WrapperBase.Access.WORM, # None or -1 if not present
             "ts": ecwb.WrapperBase.Access.WORM,
             "local_dt": ecwb.WrapperBase.Access.WORM,
             "fmt_time": ecwb.WrapperBase.Access.WORM,
             "client_app_version": ecwb.WrapperBase.Access.WORM,
             "client_os_version": ecwb.WrapperBase.Access.WORM
            }
    enums = {}
    geojson = []
    nullable = []
    local_dates = ['local_dt']

    def _populateDependencies(self):
        pass
