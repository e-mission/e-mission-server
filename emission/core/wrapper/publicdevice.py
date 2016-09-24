import logging
import emission.core.wrapper.wrapperbase as ecwb

class Publicdevice(ecwb.WrapperBase):
    props = {"ts": ecwb.WrapperBase.Access.RO  # timestamp of when the phone becomes public (first entry)
                                               # or when the phone is no longer public (second entry)
             }     
    enums = {}
    geojson = []
    nullable = []
    local_dates = []

    def _populateDependencies(self):
        pass
