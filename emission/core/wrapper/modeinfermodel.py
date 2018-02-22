import logging
import emission.core.wrapper.wrapperbase as ecwb

class ModeInferModel(ecwb.WrapperBase):
    props = {"algorithm": ecwb.WrapperBase.Access.RO,  # the algorithm for the model
             "serialized_model": ecwb.WrapperBase.Access.RO, # the model, serialized using jsonpickle
             "start_ts": ecwb.WrapperBase.Access.RO,        # timestamp of the start of the data that generated this model
             "start_fmt_time": ecwb.WrapperBase.Access.RO, # formatted string representing `start_ts`
             "end_ts": ecwb.WrapperBase.Access.RO, # timestamp of the end of the data that generated this model
             "end_fmt_time": ecwb.WrapperBase.Access.RO}  # formatted representation of `end_ts`
    enums = {}
    geojson = ["loc"]
    nullable = []
    local_dates = ['local_dt']

    def _populateDependencies(self):
        pass
