import logging
import emission.core.wrapper.wrapperbase as ecwb

class CommonTrip(ecwb.WrapperBase):
    props = {"start_loc" : ecwb.WrapperBase.Access.WORM, # _id of common_place of starting point
             "end_loc" : ecwb.WrapperBase.Access.WORM, # _id of common_place of ending point
             "trips" : ecwb.WrapperBase.Access.WORM, # List of trip_ids that are associated with this common trip
             "probabilites" : ecwb.WrapperBase.Access.WORM,  # a matrix that represents the probabilites for edge
             "user_id" : ecwb.WrapperBase.Access.WORM,
             "common_trip_id" : ecwb.WrapperBase.Access.WORM
    }


    def _populateDependencies(self):
        pass


    geojson = []
    enums = {}
    nullable = []