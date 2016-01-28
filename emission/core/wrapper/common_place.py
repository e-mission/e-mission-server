import logging
import emission.core.wrapper.wrapperbase as ecwb

class CommonPlace(ecwb.WrapperBase):
    props = {"coords" : ecwb.WrapperBase.Access.WORM, # geojson coordinates that represent the place
             "address" : ecwb.WrapperBase.Access.WORM, # address that represents the place
    }

    geojson = ["coords"]

    def _populateDependencies(self):
        pass
