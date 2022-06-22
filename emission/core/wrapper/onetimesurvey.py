from __future__ import unicode_literals
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import
from future import standard_library
standard_library.install_aliases()
from builtins import *
import logging
import emission.core.wrapper.wrapperbase as ecwb

class OneTimeSurvey(ecwb.WrapperBase):
    props = {"ts": ecwb.WrapperBase.Access.RO,    # timestamp at which the survey was taken
             "local_dt": ecwb.WrapperBase.Access.RO, # survey time in the local time zone, separated
             "fmt_time": ecwb.WrapperBase.Access.RO, # formatted survey time
             "label": ecwb.WrapperBase.Access.RO, # string summary of the survey
             "version": ecwb.WrapperBase.Access.RO, # the survey version
             "name": ecwb.WrapperBase.Access.RO, # the survey name, if we push out multiple surveys
             "xmlResponse": ecwb.WrapperBase.Access.RO, # the response XML as a string
             "jsonDocResponse": ecwb.WrapperBase.Access.RO # the response XML as JSON
    }

    enums = {}
    geojson = []
    nullable = []
    local_dates = ["start_local_dt", "end_local_dt"]

    def _populateDependencies(self):
        pass
