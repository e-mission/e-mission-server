#maps team provided get_cost function
from common.featurecalc import get_cost

class Trip(object): 
    //Instance parameters
    def __init__(self, single_mode, legs, cost, start_time, end_time, start_point, end_point)
        #may be useful for utility function, distinguishing between
        #multimodal classifier or single
        #contains the mode if only one, None if not
        self.single_mode = single_mode
        #list of Activity objects for multi-mode trips
        self.cost = cost 
        self.legs = legs
        self.start_time = start_time
        self.end_time = end_time
        self.start_point = start_point
        self.end_point = end_point

    def get_duration():
        return

    def get_distance():
        return

class E_Mission_Trip(Trip):
    #augmented parameter necessary so that we don't try and find alternatives again for a trip which previously had none
    def __init__(self, single_mode, cost, legs, start_time, end_time, start_point, end_point, alternatives=[], augmented=False): 
        super(E_Mission_Trip, self).__init__(single_mode, cost, legs, start_time, end_time, start_point, end_point, alternatives=[], augmented=False): 
        self.alternatives = alternatives
        self.augmented = augmented
    
    def get_alternatives(self):
        return self.alternatives

    def get_augmented(self):
        return self.augmented

    @staticmethod
    def trip_from_json(json_seg):
        '''
        parse json into proper trip format
        '''
        single_mode = get_single_mode()
        cost = get_cost()
        legs = get_legs()
        #start_time = 
        #end_time = 
        #start_point = 
        #end_point = 
        if augmented:
            return Trip(single_mode, cost, legs, start_time, end_time, start_point, end_point, alternatives, True)
        return Trip(single_mode, cost, legs, start_time, end_time, start_point, end_point, alternatives, False)

class Leg:
    """Represents the leg of a trip"""
    def __init__(self, trip_id):
        self.starting_point = None
        self.ending_point = None
        self.mode = None
        self.cost = 0
        self.duration = 0
        self.distance = 0
        self.dirs = None


