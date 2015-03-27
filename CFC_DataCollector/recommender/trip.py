#maps team provided get_cost function
#from common.featurecalc import get_cost

class Trip(object): 
    #Instance parameters
    def __init__(self, json_segment):
        self.trip_from_json(json_segment)

    def trip_from_json(self, json_seg):
        '''
        parse json into proper trip format
        '''
        self._id = json_seg["_id"]
        #TODO: make sure this is correct
        self.single_mode = json_seg["mode"] 
        #TODO: not sure how to get all legs from section query
        self.legs = []
        self.start_time = json_seg["section_start_time"]
        self.end_time = json_seg["section_end_time"]
        #TODO: check this field
        self.start_point = json_seg["track_points"][0]
        self.end_point = json_seg["track_points"][-1]

    def get_id():
        return self._id

    def get_duration():
        return

    def get_distance():
        return

class PipelineFlags(object):
    def __init__(self):
        self.alternativesStarted = False
        self.alternativesFinished = False

    def startAlternatives(self):
        self.alternativesStarted = True

    def finishAlternatives(self):
        self.alternativesFinished = True

class E_Mission_Trip(Trip):

    #if there are no alternatives found, set alternatives list to None 
    #def __init__(self, _id, single_mode, legs, start_time, end_time, start_point, end_point, alternatives=[], pipelineFlags = None): 
    def __init__(self, alternatives=[], pipelineFlags = None, json_segment): 
        super(self.__class__, self).__init__()
        self.alternatives = alternatives
        self.pipelineFlags = PipelineFlags()
    
    def get_alternatives(self):
        return self.alternatives

    def getPipelineFlags(self):
        return self.pipelineFlags

class Canonical_Trip(Trip):
    #if there are no alternatives found, set alternatives list to None 
    #def __init__(self, json_segment, start_point_distr, end_point_distr, start_time_distr, end_time_distr, num_trips, alternatives = []):
    def __init__(self, json_segment, num_trips, alternatives = []):
        super(self.__class__, self).__init__(json_segment)
        self.calc_start_point_distr(json_segment)
        self.calc_end_point_distr(json_segment)
        self.calc_start_time_distr(json_segment)
        self.calc_end_time_distr(json_segment)
        self.num_trips = num_trips
        self.alternatives = alternatives

    def calc_start_point_distr(json_segment):
        self.start_point_distr = None 
    def calc_end_point_distr(json_segment):
        self.end_point_distr = None 
    def calc_start_time_distr(json_segment):
        self.start_time_distr = None 
    def calc_end_time_distr(json_segment):
        self.end_time_distr = None 
    
    def get_alternatives(self):
        return self.alternatives

class Alternative_Trip(Trip):
    #def __init__(self, _id, single_mode, legs, start_time, end_time, start_point, end_point, trip_id, parent_id, cost):
    def __init__(self, trip_id, parent_id, cost, json_segment):
        super(self.__class__, self).__init__(json_segment)
        self.trip_id = trip_id
        self.parent_id = parent_id
        self.cost = cost

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

