import logging
import emission.core.wrapper.wrapperbase as ecwb

class Location(ecwb.WrapperBase):
    props = {"is_public": ecwb.WrapperBase.Access.RO,  # 1 if the phone is (or was once) registered as a public device
             "start_ts": ecwb.WrapperBase.Access.RO, # timestamp of when the phone becomes public 
             "end_ts": ecwb.WrapperBase.Access.RO,   # timestamp of when the phone is no longer public 
             }     
    enums = {}
    geojson = []
    nullable = ["start_ts", "end_ts"]
    local_dates = []

    def _populateDependencies(self):
        pass
