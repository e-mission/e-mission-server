#maps team provided get_cost function
#from common.featurecalc import get_cost
import jsonpickle
import datetime
from get_database import *

# def unit_test():
#     info = open("C:/Users/rzarrabi/Documents/school/junior/spring/trip/e-mission-server/CFC_DataCollector/tests/data/missing_trip", "r")
#     t = Trip(info.read())
#     return tt

class Trip(object): 
    #Instance parameters
    def __init__(self, json_segment):
        self.trip_from_json(json_segment)

    def trip_from_json(self, json_seg):
        print json_seg
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
        self.start_point = json_seg["track_points"][0] if json_seg["track_points"] else None
        self.end_point = json_seg["track_points"][-1] if json_seg["track_points"] else None

    def get_id(self):
        return self._id

    def get_duration():
        return

    def get_distance():
        return

    def get_start_coordinates(self):
        #TODO: return self.start_point in following string format
        #"lat,lng"        
        return '37.862591,-122.261784'

    def get_end_coordinates(self):
        #TODO: return self.end_point in following string format
        #"lat,lng"
        return '37.862591,-122.261784'


    def get_time(self):
        #TODO: convert self.start_time to datetime object
        return datetime.datetime.now()


class PipelineFlags(object):
    def __init__(self):
        self.alternativesStarted = False
        self.alternativesFinished = False

    def startAlternatives(self):
        self.alternativesStarted = True

    def finishAlternatives(self):
        self.alternativesFinished = True

    def loadPipelineFlags(self, _id):
        db = get_section_db()
        json_object = db.find_one({'_id': _id})
        if json_object != None and 'pipelineFlags' in json_object:
        
            tf = json_object['pipelineFlags']
            if tf['alternativesStarted'] == 'True':
                self.alternativesStarted = True
            if tf['alternativesFinished'] == 'True':
                self.alternativesFinished = True

    def savePipelineFlags(self, _id):
        db = get_section_db()
        json_object = db.find_one({'_id' : _id})
        json_object['pipelineFlags'] = {'alternativesStarted': self.alternativesStarted, 'alternativesFinished': self.alternativesFinished} 

class E_Mission_Trip(Trip):

    #if there are no alternatives found, set alternatives list to None 
    #def __init__(self, _id, single_mode, legs, start_time, end_time, start_point, end_point, alternatives=[], pipelineFlags = None): 
    def __init__(self, json_segment, alternatives=[], pipelineFlags = None): 
        super(E_Mission_Trip, self).__init__(json_segment)
        self.alternatives = alternatives
        self.pipelineFlags = PipelineFlags()

        #looks into database to see if pipeline flags have been set before
        self.pipelineFlags.loadPipelineFlags(json_segment['_id'])
    
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

