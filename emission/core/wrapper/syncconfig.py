from __future__ import unicode_literals
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import
from future import standard_library
standard_library.install_aliases()
from builtins import *
import logging
import emission.core.wrapper.wrapperbase as ecwb

class Syncconfig(ecwb.WrapperBase):
    props = {"sync_interval": ecwb.WrapperBase.Access.RO,  # the interval at which data is synced and the battery level is read
             "ios_device_token": ecwb.WrapperBase.Access.RO} # device_token for ios, used for registering the device to the appropriate channel
    enums = {}
    geojson = []
    nullable = []
    local_dates = []

    def _populateDependencies(self):
        pass
