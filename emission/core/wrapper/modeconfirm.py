import logging
import emission.core.wrapper.wrapperbase as ecwb

class ModeConfirm(ecwb.WrapperBase):
    props = {"mode": ecwb.WrapperBase.Access.RO,  # the confirmed mode
             "mode_name": ecwb.WrapperBase.Access.RO, # spelled out mode name, for convenience
             "section_id": ecwb.WrapperBase.Access.RO, # may be empty if confirmed at the end of a trip, before it is processed
             "start_ts": ecwb.WrapperBase.Access.RO,
             "start_local_dt": ecwb.WrapperBase.Access.RO,
             "start_fmt_time": ecwb.WrapperBase.Access.RO,
             "end_ts": ecwb.WrapperBase.Access.RO,
             "end_local_dt": ecwb.WrapperBase.Access.RO,
             "end_fmt_time": ecwb.WrapperBase.Access.RO}
    enums = {}
    geojson = []
    nullable = []
    local_dates = ['start_local_dt', 'end_local_dt']

    def _populateDependencies(self):
        pass
