#maps team provided get_cost function
#from common.featurecalc import get_cost

class Trip(object): 
    #Instance parameters
    def __init__(self, _id, single_mode, legs, cost, start_time, end_time, start_point, end_point):

        #may be useful for utility function, distinguishing between
        #multimodal classifier or single
        #contains the mode if only one, None if not
        self.single_mode = single_mode
        #list of Activity objects for multi-mode trips
        self._id = _id
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

    #if there are no alternatives found, set alternatives list to None 
    def __init__(self, _id, single_mode, legs, start_time, end_time, start_point, end_point, alternatives=[]): 
        super(E_Mission_Trip, self).__init__(_id, single_mode, legs, start_time, end_time, start_point, end_point, alternatives=[], augmented=False) 
        self.alternatives = alternatives
        self.augmented = augmented
    
    def get_alternatives(self):
        return self.alternatives

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
        return E_Mission_Trip(_id, single_mode, legs, start_time, end_time, start_point, end_point, alternatives)

class Alternative_Trip(Trip):
    def __init__(self, _id, single_mode, legs, start_time, end_time, start_point, end_point, trip_id, parent_id):
        super(Alternative_Trip, self).__init__(_id, single_mode, legs, start_time, end_time, start_point, end_point)
        self.trip_id = trip_id
        self.parent_id = parent_id

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

