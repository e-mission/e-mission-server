import logging
import emission.core.wrapper.wrapperbase as ecwb

class TourModel(ecwb.WrapperBase):

    props = {"user_id" : ecwb.WrapperBase.Access.WORM, # user_id of the E-Missions user the graph represnts 
    }

    geojson = []
    enums = {}
    nullable = []
    local_dates = []

    def _populateDependencies(self):
        pass
