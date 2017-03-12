import logging
import emission.core.wrapper.wrapperbase as ecwb

class Incident(ecwb.WrapperBase):
    props = {"loc": ecwb.WrapperBase.Access.RO,    # geojson representation of the point
             "ts": ecwb.WrapperBase.Access.RO,     # timestamp representation of the point
             "stress": ecwb.WrapperBase.Access.RO, # stress level (0 = no stress, 100 = max stress)
             "local_dt": ecwb.WrapperBase.Access.RO, # searchable datetime in local time
             "fmt_time": ecwb.WrapperBase.Access.RO #  formatted time
    }

    enums = {}
    geojson = ["loc"]
    nullable = []
    local_dates = ["local_dt"]

    def _populateDependencies(self):
        pass
