from builtins import *
import emission.core.wrapper.wrapperbase as ecwb


class Vehicle(ecwb.WrapperBase):
    props = {
        "placeholder": ecwb.WrapperBase.Access.RW,
    }

    enums = {}
    geojson = []
    nullable = []
    local_dates = []

    def _populateDependencies(self):
        pass
