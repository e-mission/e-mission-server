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
