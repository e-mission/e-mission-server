
from __future__ import unicode_literals
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import
from future import standard_library
standard_library.install_aliases()
from builtins import *
import logging
import emission.core.wrapper.wrapperbase as ecwb

class Location(ecwb.WrapperBase):
    props = {"ts": ecwb.WrapperBase.Access.RO, # timestamp the configuration was last changed
             "local_dt": ecwb.WrapperBase.Access.RO, # searchable datetime in local time
             "fmt_time": ecwb.WrapperBase.Access.RO, # formatted version of the timestampst changed
             "version": ecwb.WrapperBase.Access.RO,  # the format version of the document
             "joined": ecwb.WrapperBase.Access.RO,   # the join settings, including label and source
             "server": ecwb.WrapperBase.Access.RO,       # server customizations, notably the URL
             "intro": ecwb.WrapperBase.Access.RO,        # introduction customizations, notably the text in the summary and consent, and the auth method
             "display_config": ecwb.WrapperBase.Access.RO, # customizations for display/UI screens
             "profile_controls": ecwb.WrapperBase.Access.RO}  # customizations for entries to show in the profile

    enums = {}
    geojson = []
    nullable = []
    local_dates = ['local_dt']

    def _populateDependencies(self):
        pass
