import logging
import emission.core.wrapper.wrapperbase as ecwb


class Survey(ecwb.WrapperBase):
    props = {
        "ts": ecwb.WrapperBase.Access.RO,  # timestamp representation of the point
        "local_dt": ecwb.WrapperBase.Access.RO,  # searchable datetime in local time
        "fmt_time": ecwb.WrapperBase.Access.RO,  # formatted time
        "trip_start_ts": ecwb.WrapperBase.Access.RO,  # time stamp of associate trip
        "trip_end_ts": ecwb.WrapperBase.Access.RO,  # time stamp of associate trip
        "survey": ecwb.WrapperBase.Access.RO,  # survey data
    }

    enums = {}
    geojson = []
    nullable = []
    local_dates = ["local_dt"]

    def _populateDependencies(self):
        pass
