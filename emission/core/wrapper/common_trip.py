import logging
import emission.core.wrapper.wrapperbase as ecwb

class CommonTrip(ecwb.WrapperBase):
    props = {"start_place" : ecwb.WrapperBase.Access.WORM, # _id of common_place of starting point
             "end_place" : ecwb.WrapperBase.Access.WORM, # _id of common_place of ending point
             "start_loc" : ecwb.WrapperBase.Access.WORM, # JSON of the start location, duplicated for ease of access
             "end_loc" : ecwb.WrapperBase.Access.WORM, # JSON of the end location, duplicated for ease of access
             "trips" : ecwb.WrapperBase.Access.WORM, # List of trip_ids that are associated with this common trip
             "probabilites" : ecwb.WrapperBase.Access.WORM,  # a matrix that represents the probabilites for edge
             "user_id" : ecwb.WrapperBase.Access.WORM,
             "start_times" : ecwb.WrapperBase.Access.WORM,
             "durations" : ecwb.WrapperBase.Access.WORM}


    def _populateDependencies(self):
        pass

    geojson = ['start_loc', 'end_loc']
    enums = {}
    nullable = []
