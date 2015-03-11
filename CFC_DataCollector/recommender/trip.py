#maps team provided get_cost function
from maps import get_cost
import common
class Trip: 
    //Instance parameters
    def __init__(self, single_mode, cost, legs, start_time, end_time, start_point, end_point, alternatives=[], augmented=False): 
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
        self.alternatives = alternatives
        self.augmented = False

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

    def get_duration():
        return

    def get_distance():
        return


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


