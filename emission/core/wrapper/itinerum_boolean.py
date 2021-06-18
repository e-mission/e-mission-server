import emission.core.wrapper.wrapperbase as ecwb

class ItinerumBoolean(ecwb.WrapperBase):
    props = { "trip_id": ecwb.WrapperBase.Access.WORM,
            "itinerum_boolean": ecwb.WrapperBase.Access.WORM,
            "start_ts": ecwb.WrapperBase.Access.WORM,
            "end_ts": ecwb.WrapperBase.Access.WORM,
            }
    enums = {}
    geojson = {}
    local_dates {}

    def _populateDependencies(self):
        pass
