import logging
import emission.core.wrapper.wrapperbase as ecwb

class CommonTrip(ecwb.WrapperBase):
    props = {"start_loc" : ecwb.WrapperBase.Access.WORM, # Lat/lng start point of trip
             "end_loc" : ecwb.WrapperBase.Access.WORM, # Lat/lng end point of trip
             "trips" : ecwb.WrapperBase.Access.WORM, # List of trip_ids that are associated with this common trip
             "probabilites" : ecwb.WrapperBase.Access.WORM  # a matrix that represents the probabilites for edge
    }

    geojson = ["start_loc", "end_loc"]

    def _populateDependencies(self):
        pass
