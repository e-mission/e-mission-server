"""
Placeholder dock wrapper for modifiable state examples.
"""
from builtins import *
import emission.core.wrapper.wrapperbase as ecwb


class Dock(ecwb.WrapperBase):
    props = {
        "dock_id": ecwb.WrapperBase.Access.RW,
        "dock_name": ecwb.WrapperBase.Access.RW,
    }

    enums = {}
    geojson = []
    nullable = []
    local_dates = []

    def _populateDependencies(self):
        pass
