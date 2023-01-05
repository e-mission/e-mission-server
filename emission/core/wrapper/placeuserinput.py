from __future__ import unicode_literals
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import
from future import standard_library
standard_library.install_aliases()
from builtins import *
import logging
import emission.core.wrapper.wrapperbase as ecwb

class Placeuserinput(ecwb.WrapperBase):
    props = {"enter_ts": ecwb.WrapperBase.Access.RO,    # geojson representation of the point
             "enter_local_dt": ecwb.WrapperBase.Access.RO, # start datetime in local time
             "enter_fmt_time": ecwb.WrapperBase.Access.RO, # start formatted time (in timezone of point)
             "exit_ts": ecwb.WrapperBase.Access.WORM, # end UTC timestamp (in secs)
             "exit_local_dt": ecwb.WrapperBase.Access.RO, # end datetime in local time
             "exit_fmt_time": ecwb.WrapperBase.Access.RO, # end formatted time (in timezone of point)
             "label": ecwb.WrapperBase.Access.RO, # string summary of the survey
             "version": ecwb.WrapperBase.Access.RO, # the survey version
             "name": ecwb.WrapperBase.Access.RO, # the survey name
             "xmlResponse": ecwb.WrapperBase.Access.RO, # the XML string representation of the survey response
             "jsonDocResponse": ecwb.WrapperBase.Access.RO # the JSON representation of the survey response
    }

    enums = {}
    geojson = []
    nullable = []
    local_dates = ["enter_local_dt", "exit_local_dt"]

    def _populateDependencies(self):
        pass
