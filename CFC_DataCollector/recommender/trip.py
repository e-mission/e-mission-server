#maps team provided get_cost function
#from common.featurecalc import get_cost
from common import Coordinate
import jsonpickle
import datetime
from get_database import *

DATE_FORMAT = "%Y%m%dT%H%M%S-0700"

class Trip(object):

    def __init__(self, json_segment):
        self._id = json_segment.get("_id")
        self.user_id = json_segment.get("user_id")
        self.trip_id = json_segment.get("trip_id")
        self.sections = self._init_sections(self.user_id, self.trip_id, len(json_segment.get("sections"))) if json_segment.get("sections") else None
        print json_segment
        self.trip_start_time = datetime.datetime.strptime(json_segment.get("trip_start_time"), DATE_FORMAT)
        self.trip_end_time = datetime.datetime.strptime(json_segment.get("trip_end_time"), DATE_FORMAT)
        self.trip_start_location = self._start_location(self.sections)
        self.trip_end_location = self._end_location(self.sections)
    
    def _init_sections(self, user_id, trip_id, num_sections):
        sections = []
        db = get_section_db()
        json_object = db.find({'user_id': user_id, 'trip_id' : trip_id}, limit = num_sections)
        for section_json in json_object:
            sections.append(Section(section_json))
        return sections 

    def _start_location(self, sections):
        return sections[0].section_start_location if sections else None
         
    def _end_location(self, sections):
        return sections[-1].section_end_location if sections else None

    def save_to_db(self):
        pass

class Section(object):

    def __init__(self, json_segment):
        self._id = json_segment.get("_id")
        self.trip_id = json_segment.get("trip_id")
        self.distance = json_segment.get("distance")
        self.section_start_time = datetime.datetime.strptime(json_segment.get("section_start_time"), DATE_FORMAT)
        self.section_end_time = datetime.datetime.strptime(json_segment.get("section_end_time"), DATE_FORMAT)
        self.section_start_location = self._start_location(json_segment.get("track_points"))
        self.section_end_location = self._end_location(json_segment.get("track_points"))
        self.mode = json_segment.get("mode")
        self.confirmed_mode = json_segment.get("confirmed_mode")

    def _start_location(self, points):
        return Coordinate(points[0]["track_location"]["coordinates"][1], points[0]["track_location"]["coordinates"][0]) if points else None

    def _end_location(self, points):
        return Coordinate(points[-1]["track_location"]["coordinates"][1], points[-1]["track_location"]["coordinates"][0]) if points else None

    def save_to_db(self):
        db = get_section_db()
        db.update_one({"_id": self._id}, 
                      {"$set": {"distance" : self.distance, "mode" : self.mode, "confirmed_mode" : self.confirmed_mode}},
                       upsert=False)

class E_Mission_Trip(Trip):

    def __init__(self, json_segment, pipelineFlags = None): 
        super(E_Mission_Trip, self).__init__(json_segment)
        self.subtype = None
        self.alternatives = self._init_alternatives(self.user_id, self.trip_id, len(json_segment.get("alternatives"))) if json_segment.get("alternatives") else None
        self.perturbed_trips = self._init_perturbed(self.user_id, self.trip_id, len(json_segment.get("perturbed"))) if json_segment.get("perturbed") else None
        self.pipelineFlags = PipelineFlags(self._id)
        self.single_mode = self._init_single_mode(self.sections)

        #looks into database to see if pipeline flags have been set before
        self.pipelineFlags.loadPipelineFlags(json_segment['_id'])

    def _init_alternatives(self, user_id, trip_id, num_alternatives):
        alternatives = []
        db = get_alternatives_db()
        json_object = db.find({'user_id' : user_id, 'trip_id' : trip_id}, limit = num_alternatives)
        for alternative_json in json_object:
            alternatives.append(Alternative_Trip(alternative_json))
        return alternatives 

    def _init_perturbed(self, user_id, trip_id, num_perturbed):
        perturbed = []
        db = get_perturbed_db()
        json_object = db.find({'user_id' : user_id, 'trip_id' : trip_id}, limit = num_perturbed)
        for perturbed_json in json_object:
            perturbed.append(Perturbed_Trip(perturbed_json))
        return perturbed 

    def _init_single_mode(self, sections):
        if not sections:
            return None
        mode = sections[0].mode
        for section in sections:
            if section.mode != mode:
                return None
        return mode

    def save_to_db(self):
        db = get_trip_db()
        db.update_one({"_id": self._id}, 
                      {"$set": {"distance" : self.distance, "mode" : self.mode, "confirmed_mode" : self.confirmed_mode}},
                       upsert=False)
        self.save_alternatives(self.alternatives)
        self.save_perturbed(self.perturbed)

    def _save_alternatives(alternatives):
        for alternative in alternatives:
            alternative.save_to_db()

    def _save_perturbed(perturbeds):
        for perturbed in perturbeds:
            perturbed.save_to_db()

class Canonical_E_Mission_Trip(E_Mission_Trip):
    #if there are no alternatives found, set alternatives list to None 
    def __init__(self, json_segment):
        super(self.__class__, self).__init__(json_segment)
        self.start_point_distr = json_segment.get("start_point_distr")
        self.end_point_distr = json_segment.get("end_point_distr")
        self.start_time_distr = json_segment.get("start_time_distr")
        self.end_time_distr = json_segment.get("end_time_distr")

class Alternative_Trip(Trip):
    def __init__(self, json_segment):
        super(self.__class__, self).__init__(json_segment)
        self.parent_id = json_segment.get("parent_id")
        self.cost = json_segment.get("cost")

class Canonical_Alternative_Trip(Alternative_Trip):
    def __init__(self, canonical_trip_id, cost, json_segment):
        super(self.__class__, self).__init__(json_segment)
        self.subtype = "canonical_alternative"

class Perturbed_Trip(Alternative_Trip, E_Mission_Trip):
    def __init__(self, json_segment):
        super(self.__class__, self).__init__(json_segment)
        self.subtype = "perturbed"

class PipelineFlags(object):
    def __init__(self, _id):
        self.alternativesStarted = False
        self.alternativesFinished = False
        self._id = _id
        self.loadPipelineFlags(self._id)

    def startAlternatives(self):
        self.alternativesStarted = True

    def finishAlternatives(self):
        self.alternativesFinished = True

    def loadPipelineFlags(self, _id):
        db = get_trip_db()
        json_object = db.find_one({'_id': _id})
        if json_object:
            tf = json_object.get('pipelineFlags')
            if tf:
                if tf['alternativesStarted'] == 'True':
                    self.alternativesStarted = True
                if tf['alternativesFinished'] == 'True':
                    self.alternativesFinished = True
    
    def savePipelineFlags(self, _id):
        db = get_trip_db()
        db.update_one({"_id": self._id}, 
                      {"$set": {"pipelineFlags" : {'alternativesStarted': self.alternativesStarted, 'alternativesFinished': self.alternativesFinished}}},
                       upsert)

