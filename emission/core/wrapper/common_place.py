import logging
import emission.core.wrapper.wrapperbase as ecwb

class CommonPlace(ecwb.WrapperBase):
    props = {
         "coords" : ecwb.WrapperBase.Access.WORM, # geojson coordinates that represent the place
         "successors" : ecwb.WrapperBase.Access.WORM, # set of CommonPlaces connected to this location
         "user_id" : ecwb.WrapperBase.Access.WORM,
         "common_place_id" : ecwb.WrapperBase.Access.WORM
    }

    geojson = ["coords"]
    enums = {}
    nullable = []

    def _populateDependencies(self):
        pass
