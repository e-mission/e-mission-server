import logging
import emission.core.wrapper.wrapperbase as ecwb

class Userlabel(ecwb.WrapperBase):
    props = {"start_ts": ecwb.WrapperBase.Access.RO,    # geojson representation of the point
             "start_local_dt": ecwb.WrapperBase.Access.RO, # start datetime in local time
             "start_fmt_time": ecwb.WrapperBase.Access.RO, # start formatted time (in timezone of point)
             "end_ts": ecwb.WrapperBase.Access.WORM, # end UTC timestamp (in secs)
             "end_local_dt": ecwb.WrapperBase.Access.RO, # end datetime in local time
             "end_fmt_time": ecwb.WrapperBase.Access.RO, # end formatted time (in timezone of point)
             "label": ecwb.WrapperBase.Access.RO # string representation of mode
    }

    enums = {}
    geojson = []
    nullable = []
    local_dates = ["start_local_dt", "end_local_dt"]

    def _populateDependencies(self):
        pass
