import logging
import emission.core.wrapper.wrapperbase as ecwb

class TourModel(ecwb.WrapperBase):

    props = {"user_id" : ecwb.WrapperBase.Access.WORM, # user_id of the E-Missions user the graph represnts 
             "commonPlaces" : ecwb.WrapperBase.Access.WORM, # a list of CommonPlace objects in ths tm
             "commonTrips" : ecwb.WrapperBase.Access.WORM  # A list of CommonTrip objects in this tm
    }

    def _populateDependencies(self):
        pass
