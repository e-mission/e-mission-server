#maps team provided get_cost function
#from common.featurecalc import get_cost
from common import Coordinate
import jsonpickle
import datetime
from get_database import *

# def unit_test():
#     info = open("C:/Users/rzarrabi/Documents/school/junior/spring/trip/e-mission-server/CFC_DataCollector/tests/data/missing_trip", "r")
#     t = Trip(info.read())
#     return tt

def trip_factory(json_segment):
    #E_Mission_Trip
    if json_segment.get("alternatives"):
        if json_segment.get("start_point_distr"):
            return Canonical_E_Mission_Trip(json_segment)
        else:
            return E_Mission_Trip(json_segment)
    #Alternative Trip
    else:
        #Canonical Alternative   
        if json_segment.get("canonical"):
            return Canonical_Alternative_Trip(json_segment)
        elif json_segment.get("perturbed"):
            return Perturbed_Trip(json_segment)
        else:
            return Alternative_Trip(json_segment)

        

class Trip(object): 
    #Instance parameters
    def __init__(self, json_segment):
        self.trip_from_json(json_segment)

    def trip_from_json(self, json_seg):
        self._id = json_seg["_id"]
        #TODO: make sure this is correct
        self.single_mode = json_seg["mode"] 
        self.start_time = json_seg["section_start_time"]
        self.end_time = json_seg["section_end_time"]
        #TODO: check this field
        points = json_seg["track_points"]
        self.start_point = Coordinate(points[0][1], points[0][0]) if points else None
        self.end_point = Coordinate(points[-1][1], points[-1][0]) if points else None
        print self.__dict__

    def save_to_db(self, collection):
        attrs = self.__dict__
        collection.insert(attrs)

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
        self.loadPipelineFlags()

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

    def saveToDb(self, collection):
        db = get_section_db()
        json_object = db.find_one({'_id' : self._id})
        json_object['pipelineFlags'] = {'alternativesStarted': self.alternativesStarted, 'alternativesFinished': self.alternativesFinished} 
    

class Canonical_E_Mission_Trip(E_Mission_Trip):
    #if there are no alternatives found, set alternatives list to None 
    #def __init__(self, json_segment, start_point_distr, end_point_distr, start_time_distr, end_time_distr, num_trips, alternatives = []):
    def __init__(self, json_segment, alternatives = [], pipelineFlags = None):
        super(self.__class__, self).__init__(json_segment, alternatives, pipelineFlags)
        self.start_point_distr = json_segment["start_point_distr"]
        self.end_point_distr = json_segment["end_point_distr"]
        self.start_time_distr = json_segment["start_time_distr"]
        self.end_time_distr = json_segment["end_time_distr"]
    
    def get_alternatives(self):
        return self.alternatives

    def get_num_trips(self):
        #TODO: make sure this is consistent with generation
        return len(self.start_point_distr)

class Alternative_Trip(Trip):
    def __init__(self, json_segment):
        super(self.__class__, self).__init__(json_segment)
        self.parent_id = json_segment["parent_id"]
        self.cost = json_segment.get("cost")

class Canonical_Alternative_Trip(Alternative_Trip):
    def __init__(self, canonical_trip_id, cost, json_segment):
        super(self.__class__, self).__init__(json_segment)
        self.canonical = True

class Perturbed_Trip(Alternative_Trip, E_Mission_Trip):
    def __init__(self, json_segment):
        super(self.__class__, self).__init__(json_segment)

