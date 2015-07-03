#maps team provided get_cost function
#from common.featurecalc import get_cost
import datetime
from get_database import *
import sys
import os
sys.path.append("%s/../CFC_WebApp/" % os.getcwd())
from main import common as cm
import logging

DATE_FORMAT = "%Y%m%dT%H%M%S-%W00"

class Coordinate:
    def __init__(self, lat, lon):
        self.lat = lat
        self.lon = lon

    def get_lat(self):
        return self.lat

    def get_lon(self):
        return self.lon

    def maps_coordinate(self):
        return str((float(self.lat), float(self.lon)))

    def coordinate_list(self):
        return [float(self.lon), float(self.lat)]

    def __str__(self):
        return self.maps_coordinate()

    def __repr__(self):
        return self.maps_coordinate()

class Trip(object):

    def __init__(self, _id, user_id, trip_id, sections, start_time, end_time, trip_start_location, trip_end_location):
        self._id = _id
        self.user_id = user_id
        self.trip_id = trip_id
        self.sections = sections
        self.start_time = start_time
        self.end_time = end_time
        self.trip_start_location = trip_start_location
        self.trip_end_location = trip_end_location

    @classmethod
    def trip_from_json(cls, json_segment):
        _id = json_segment.get("_id")
        user_id = json_segment.get("user_id")
        trip_id = json_segment.get("trip_id")
        sections = cls._init_sections(user_id, trip_id, len(json_segment.get("sections"))) if json_segment.get("sections") else None
        try:
            start_time = json_segment["trip_start_datetime"]
            end_time = json_segment["trip_end_datetime"]
        except:
            start_time = datetime.datetime.strptime(json_segment.get("trip_start_time"), DATE_FORMAT)
            end_time = datetime.datetime.strptime(json_segment.get("trip_end_time"), DATE_FORMAT)
        trip_start_location = cls._start_location(sections)
        trip_end_location = cls._end_location(sections)
        return cls(_id, user_id, trip_id, sections, start_time, end_time, trip_start_location, trip_end_location)

    @classmethod
    def _init_sections(cls, user_id, trip_id, num_sections):
        sections = []
        db = get_section_db()
        json_object = db.find({'user_id': user_id, 'trip_id' : trip_id}, limit = num_sections)
        for section_json in json_object:
            sections.append(Section.section_from_json(section_json))
        return sections

    @classmethod
    def _start_location(cls, sections):
        return sections[0].section_start_location if sections else None

    @classmethod
    def _end_location(cls, sections):
        return sections[-1].section_end_location if sections else None

    def get_duration(self):
        # return duration
        return self.end_time - self.start_time

    def get_distance(self):
        return cm.calDistance(self.trip_start_location, self.trip_end_location, True)

    def save_to_db(self):
        pass


class Section(object):

    def __init__(self, _id, user_id, trip_id, distance, section_type, start_time, end_time, section_start_location, section_end_location, mode, confirmed_mode):
        self._id = _id
        self.user_id = user_id
        self.trip_id = trip_id
        self.distance = distance
        self.section_type = section_type
        self.start_time = start_time
        self.end_time = end_time
        self.section_start_location = section_start_location
        self.section_end_location = section_end_location
        self.mode = mode
        self.confirmed_mode = confirmed_mode
        self.points = []

    def __str__(self):
        return "%s:%s:%s" % (self.trip_id, self._id, self.section_type)

    @classmethod
    def section_from_json(cls, json_segment):
        _id = json_segment.get("_id")
        user_id = json_segment.get("user_id")
        trip_id = json_segment.get("trip_id")
        distance = json_segment.get("distance")
        section_type = json_segment.get("type")
        # This used to have a fallback to parsing the section_start_time, and section_end_time.
        # However, we don't actually have any sections that would use the
        # fallback, and our current code always parses the section times before
        # storing to the database, so the complexity is not needed
        # In [311]: get_section_db().find({'section_start_datetime': {'$exists': False}}).count()
        # Out[311]: 0
        # In [312]: get_section_db().find({'section_start_datetime': {'$exists': True}}).count()
        # Out[312]: 97671
        # In [313]: get_section_db().find({'section_end_datetime': {'$exists': False}}).count()
        # Out[313]: 0
        # In [314]: get_section_db().find({'section_end_datetime': {'$exists': True}}).count()
        # Out[314]: 97671
        # In [315]: get_section_db().find().count()
        # Out[315]: 97671

        start_time = cls._get_datetime(json_segment, "section_start_datetime")
        end_time = cls._get_datetime(json_segment, "section_end_datetime")
        section_start_location = cls._get_coordinate(json_segment, "section_start_point")
        section_end_location = cls._get_coordinate(json_segment, "section_end_point")
        mode = json_segment.get("mode")
        confirmed_mode = json_segment.get("confirmed_mode")
        return cls(_id, user_id, trip_id, distance, section_type, start_time, end_time, section_start_location, section_end_location, mode, confirmed_mode)

    @classmethod
    def _get_coordinate(cls, json_segment, coord_key):
        logging.debug("While retrieving %s from %s, in is %s" % (coord_key, json_segment, (coord_key in json_segment)))
        if coord_key in json_segment and json_segment[coord_key] is not None:
            if "coordinates" in json_segment[coord_key]:
                coord_json = json_segment[coord_key]["coordinates"]
                return Coordinate(coord_json[1], coord_json[0])
            else:
                logging.warn("Unable to get coordinates from key %s in segment %s " % (coord_key, json_segment))
                return None
        else:
            return None

    @classmethod
    def _get_datetime(cls, json_segment, coord_key):
        if coord_key in json_segment:
            return json_segment[coord_key]
        else:
            return None

    def save_to_db(self):
        db = get_section_db()
        db.update({"_id": self._id},
                      {"$set": {"distance" : self.distance, "mode" : self.mode, "confirmed_mode" : self.confirmed_mode}},
                       upsert=False, multi=False)


class E_Mission_Trip(Trip):

    def __init__(self, _id, user_id, trip_id, sections, start_time, end_time, trip_start_location, trip_end_location, alternatives, perturbed_trips,
                 mode_list, confirmed_mode_list, recommended_alternative=None):
        super(E_Mission_Trip, self).__init__(_id, user_id, trip_id, sections, start_time, end_time, trip_start_location, trip_end_location)
        self.alternatives = alternatives
        self.perturbed_trips = perturbed_trips
        self.mode_list = mode_list
        self.confirmed_mode_list = confirmed_mode_list
        self.subtype = None
        #Loads or initializes pipeline flags for trip
        self.pipelineFlags = PipelineFlags(self._id)
        self.recommended_alternative = recommended_alternative

    @classmethod
    def trip_from_json(cls, json_segment):
        trip = Trip.trip_from_json(json_segment)
        trip.subtype = None
        trip.alternatives = cls._init_alternatives(trip.user_id, trip.trip_id, len(json_segment.get("alternatives"))) if json_segment.get("alternatives") else None
        trip.perturbed_trips = cls._init_perturbed(trip.user_id, trip.trip_id, len(json_segment.get("perturbed"))) if json_segment.get("perturbed") else None
        trip.mode_list = cls._init_mode_list(trip.sections)
        trip.confirmed_mode_list = cls._init_confirmed_mode_list(trip.sections)
        trip.pipelineFlags = PipelineFlags(trip.trip_id)
        trip.recommended_alternatives = json_segment.get("recommended_alternatives")
        return cls(trip._id, trip.user_id, trip.trip_id, trip.sections, trip.start_time, trip.end_time, trip.trip_start_location, trip.trip_end_location, trip.alternatives, trip.perturbed_trips, trip.mode_list, trip.confirmed_mode_list)

    @classmethod
    def _init_alternatives(self, user_id, trip_id, num_alternatives):
        alternatives = []
        db = get_alternatives_db()
        json_object = db.find({'user_id' : user_id, 'trip_id' : trip_id}, limit = num_alternatives)
        for alternative_json in json_object:
            alternatives.append(Alternative_Trip(alternative_json))
        return alternatives

    @classmethod
    def _init_perturbed(self, user_id, trip_id, num_perturbed):
        perturbed = []
        db = get_perturbed_db()
        json_object = db.find({'user_id' : user_id, 'trip_id' : trip_id}, limit = num_perturbed)
        for perturbed_json in json_object:
            perturbed.append(Perturbed_Trip(perturbed_json))
        return perturbed

    @classmethod
    def _init_mode_list(self, sections):
        if not sections:
            return None
        mode_list = []
        mode_set = set()
        for section in sections:
            mode_list.append(section.mode)
            mode_set.add(section.mode)
        if len(mode_set) == 1:
            return mode_set.pop()
        return mode_list

    @classmethod
    def _init_confirmed_mode_list(self, sections):
        if not sections:
            return None
        mode_list = []
        mode_set = set()
        for section in sections:
            mode_list.append(section.confirmed_mode)
            mode_set.add(section.confirmed_mode)
        if len(mode_set) == 1:
            return mode_set.pop()

    def mark_recommended(self, alternative):
        db = get_trip_db()
        '''
        point_list = []
        for section in self.sections:
            point_list.append([{'coordinates':[point.lon, point.lat]}
                                for point in section.points])
        '''
        alternative_json = {"user_id": alternative.user_id, "trip_id": alternative.trip_id,
            "trip_start_time": alternative.start_time.strftime(DATE_FORMAT),
            "trip_end_time": alternative.end_time.strftime(DATE_FORMAT),
            "trip_start_location": alternative.trip_start_location.coordinate_list(),
            "trip_end_location": alternative.trip_end_location.coordinate_list(),
            "mode_list": alternative.mode_list,
            "track_points": alternative.track_points}
        print "recommending"
        result = db.update({"trip_id": self.trip_id, "user_id": self.user_id},
                      {"$set": {"recommended_alternative" : alternative_json}},
                       upsert=False,multi=False)

    def save_to_db(self):
        db = get_trip_db()
        result = db.update({"_id": self._id},
                      {"$set": {"mode" : self.mode_list, "confirmed_mode" : self.confirmed_mode_list}},
                       upsert=False,multi=False)
        print result
        if not result["updatedExisting"]:
            self._create_new(db)
        self._save_alternatives(self.alternatives)
        self._save_perturbed(self.perturbed_trips)

    def _create_new(self, db):
        db.insert({"_id": self._id, "user_id": self.user_id,
                "trip_id": self.trip_id, "sections": self.sections, "trip_start_time": self.start_time,
                "trip_end_time": self.end_time, "trip_start_location": self.trip_start_location, "trip_end_location": self.trip_end_location,
                "alternatives": list(range(self.alternatives)), "perturbed_trips": list(range(self.perturbed_trips)),
                "mode": self.mode, "confirmed_mode": self.confirmed_mode})

    def _save_alternatives(self, alternatives):
        if alternatives:
            for alternative in alternatives:
                alternative.save_to_db()

    def _save_perturbed(self, perturbeds):
        if perturbeds:
            for perturbed in perturbeds:
                perturbed.save_to_db()

class Canonical_E_Mission_Trip(E_Mission_Trip):
    #if there are no alternatives found, set alternatives list to None 
    def __init__(self, _id, user_id, trip_id, sections, start_time, end_time, trip_start_location, trip_end_location, 
                 alternatives, perturbed_trips, mode_list, start_point_distr, end_point_distr, start_time_distr, end_time_distr, confirmed_mode_list): # added confirmed_mode_list to this constructor
        super(Canonical_E_Mission_Trip, self).__init__(_id, user_id, trip_id, sections, start_time, end_time, trip_start_location, trip_end_location, 
            alternatives, perturbed_trips, mode_list, confirmed_mode_list) # super expects more arguments than this
        self.start_point_distr = start_point_distr
        self.end_point_distr = end_point_distr
        self.start_time_distr = start_time_distr
        self.end_time_distr = end_time_distr

    @classmethod
    def trip_from_json(cls, json_segment):
        trip = Section.section_from_json(json_segment.get("representative_trip"))
        start_point_distr = json_segment.get("start_point_distr")
        end_point_distr = json_segment.get("end_point_distr")
        start_time_distr = json_segment.get("start_time_distr")
        end_time_distr = json_segment.get("end_time_distr")
        confirmed_mode_list = json_segment.get("confirmed_mode_list")
        return cls(trip._id, trip.user_id, trip.trip_id, [], trip.start_time, trip.end_time, trip.section_start_location, trip.section_end_location, 
                   [], [], [], start_point_distr, end_point_distr, 
                   start_time_distr, end_time_distr, confirmed_mode_list)

    def save_to_db(self):
        db = get_canonical_trips_db()
        result = db.update({"_id": self._id},
                {"$set": {"start_point_distr" : self.start_point_distr, "end_point_distr" : self.end_point_distr, "start_time_distr": self.start_time_distr,
                    "end_time_distr": self.end_time_distr}},
                       upsert=False,multi=False)
        print result
        if not result["updatedExisting"]:
            self._create_new(db)
        self._save_alternatives(self.alternatives)
        self._save_perturbed(self.perturbed_trips)
        

class Alternative_Trip(Trip):
    def __init__(self, _id, user_id, trip_id, sections, start_time, end_time, trip_start_location, trip_end_location, parent_id, cost, mode_list, track_points=None):
        super(self.__class__, self).__init__(_id, user_id, trip_id, sections, start_time, end_time, trip_start_location, trip_end_location)
        self.subtype = "alternative"
        self.parent_id = parent_id
        self.cost = cost
        self.mode_list = mode_list

        self.trip_start_location = trip_start_location
        self.trip_end_location = trip_end_location

        self.track_points = track_points

    @classmethod
    def trip_from_json(cls, json_segment):
	# print "json_segment = %s" % json_segment
        trip = Trip.trip_from_json(json_segment)
        trip.parent_id = json_segment.get("parent_id")
        trip.cost = json_segment.get("cost")
        #trip.mode_list = cls._init_mode_list(trip.sections)
        trip.mode_list = json_segment.get("mode_list")

        trip.track_points = json_segment.get("track_points")

	trip.trip_start_location = Coordinate(json_segment.get("trip_start_location")[1], json_segment.get("trip_start_location")[0])

        trip.trip_end_location = Coordinate(json_segment.get("trip_end_location")[1], json_segment.get("trip_end_location")[0])

        return cls(trip._id, trip.user_id, trip.trip_id, trip.sections, trip.start_time, trip.end_time, trip.trip_start_location, trip.trip_end_location,
                   trip.parent_id, trip.cost, trip.mode_list, trip.track_points)

    @classmethod
    def _init_mode_list(self, sections):
        if not sections:
            return None
        mode_list = []
        mode_set = set()
        for section in sections:
            mode_list.append(section.mode)
            mode_set.add(section.mode)
        if len(mode_set) == 1:
            return mode_set.pop()
        print mode_list
        return mode_list

    '''
    def mark_recommended(self):
        db = get_alternatives_db()
        #Unique key is combination of trip, user, and mode. Only one alternative per mode
        result = db.update({"trip_id": self.trip_id, "user_id": self.user_id, "mode_list":self.mode_list},
                      {"$set": {"recommended" : True}},
                       upsert=False,multi=False)
    '''

    def save_to_db(self):
        db = get_alternatives_db()
        #Unique key is combination of trip, user, and mode. Only one alternative per mode
        result = db.update({"trip_id": self.trip_id, "user_id": self.user_id, "mode_list":self.mode_list},
                      {"$set": {"cost" : self.cost}},
                       upsert=False,multi=False)
        #print result
        if not result["updatedExisting"]:
            self._create_new(db)

    def _create_new(self, db):
        point_list = []
        for section in self.sections:
            point_list.append([{'coordinates':[point.lon, point.lat]}
                                for point in section.points])
        self._id = db.insert({"user_id": self.user_id, "trip_id": self.trip_id,
            "trip_start_time": self.start_time.strftime(DATE_FORMAT),
            "trip_end_time": self.end_time.strftime(DATE_FORMAT),
            "trip_start_location": self.trip_start_location.coordinate_list(),
            "trip_end_location": self.trip_end_location.coordinate_list(),
            "mode_list": self.mode_list,
            "track_points": point_list})


class Fake_Trip(Trip):
    """docstring for Fake_Trip"""
    def __init__(self, _id, user_id, trip_id, sections, start_time, end_time, trip_start_location, trip_end_location):
        super(self.__class__, self).__init__(_id, user_id, trip_id, sections, start_time, end_time, trip_start_location, trip_end_location)
    
    def save_to_db(self):
        db = get_fake_trips_db()
        print "trip start loc is %s" % self.trip_start_location
        print "trip end loc is %s" % self.trip_end_location 
        db.insert({"trip_start_location" : self.trip_start_location.coordinate_list(), 
            "trip_end_location" : self.trip_end_location.coordinate_list()})

class Canonical_Alternative_Trip(Alternative_Trip):
    def __init__(self, _id, user_id, trip_id, sections, start_time, end_time, trip_start_location, trip_end_location, alternatives, perturbed_trips, mode_list):
        super(self.__class__, self).__init__(_id, user_id, trip_id, sections, start_time, end_time, trip_start_location, trip_end_location, alternatives,
                                             perturbed_trips, mode_list)
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

    def savePipelineFlags(self):
        db = get_trip_db()
        db.update({"_id": self._id},
                      {"$set": {"pipelineFlags" : {'alternativesStarted': self.alternativesStarted, 'alternativesFinished': self.alternativesFinished}}},
                       multi=False, upsert=False)

