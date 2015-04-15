#maps team provided get_cost function
#from common.featurecalc import get_cost
import datetime
from get_database import *
#from feature_calc

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

    def __str__(self):
        return s

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
            start_time = datetime.datetime.strptime(json_segment.get("trip_start_time"), DATE_FORMAT)
            end_time = datetime.datetime.strptime(json_segment.get("trip_end_time"), DATE_FORMAT)
	except:
            start_time = json_segment.get("trip_start_time")
            end_time = json_segment.get("trip_end_time")
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
	'''
	return duration
	'''
        return self.end_time - self.start_time

    def get_distance(self):
	return

    def save_to_db(self):
        pass

class Section(object):

    def __init__(self, _id, trip_id, distance, start_time, end_time, section_start_location, section_end_location, mode, confirmed_mode):
        self._id = _id
        self.trip_id = trip_id
        self.distance = distance
        self.start_time = start_time
        self.end_time = end_time
        self.section_start_location = section_start_location
        self.section_end_location = section_end_location
        self.mode = mode
        self.confirmed_mode = confirmed_mode

    @classmethod
    def section_from_json(cls, json_segment):
        _id = json_segment.get("_id")
        trip_id = json_segment.get("trip_id")
        distance = json_segment.get("distance")
        start_time = datetime.datetime.strptime(json_segment.get("section_start_time"), DATE_FORMAT)
        end_time = datetime.datetime.strptime(json_segment.get("section_end_time"), DATE_FORMAT)
        section_start_location = cls._start_location(json_segment.get("track_points"))
        section_end_location = cls._end_location(json_segment.get("track_points"))
        mode = json_segment.get("mode")
        confirmed_mode = json_segment.get("confirmed_mode")
        return cls(_id, trip_id, distance, start_time, end_time, section_start_location, section_end_location, mode, confirmed_mode)

    @classmethod
    def _start_location(cls, points):
        return Coordinate(points[0]["track_location"]["coordinates"][1], points[0]["track_location"]["coordinates"][0]) if points else None

    @classmethod
    def _end_location(cls, points):
        return Coordinate(points[-1]["track_location"]["coordinates"][1], points[-1]["track_location"]["coordinates"][0]) if points else None

    def save_to_db(self):
        db = get_section_db()
        db.update({"_id": self._id},
                      {"$set": {"distance" : self.distance, "mode" : self.mode, "confirmed_mode" : self.confirmed_mode}},
                       upsert=False, multi=False)


class E_Mission_Trip(Trip):

    def __init__(self, _id, user_id, trip_id, sections, start_time, end_time, trip_start_location, trip_end_location, alternatives, perturbed_trips,
                 mode_list, confirmed_mode_list):
        super(E_Mission_Trip, self).__init__(_id, user_id, trip_id, sections, start_time, end_time, trip_start_location, trip_end_location)
        self.alternatives = alternatives
        self.perturbed_trips = perturbed_trips
        self.mode_list = mode_list
        self.confirmed_mode_list = confirmed_mode_list
        self.subtype = None
        #Loads or initializes pipeline flags for trip
        self.pipelineFlags = PipelineFlags(self._id)

    @classmethod
    def trip_from_json(cls, json_segment):
        trip = Trip.trip_from_json(json_segment)
        trip.subtype = None
        trip.alternatives = cls._init_alternatives(trip.user_id, trip.trip_id, len(json_segment.get("alternatives"))) if json_segment.get("alternatives") else None
        trip.perturbed_trips = cls._init_perturbed(trip.user_id, trip.trip_id, len(json_segment.get("perturbed"))) if json_segment.get("perturbed") else None
        trip.mode_list = cls._init_mode_list(trip.sections)
        trip.confirmed_mode_list = cls._init_confirmed_mode_list(trip.sections)
        trip.pipelineFlags = PipelineFlags(trip.trip_id)
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
                 alternatives, perturbed_trips, mode_list, start_point_distr, end_point_distr, start_time_distr, end_time_distr):
        super(Canonical_E_Mission_Trip, self).__init__(_id, user_id, trip_id, sections, start_time, end_time, trip_start_location, trip_end_location)
        self.start_point_distr = start_point_distr
        self.end_point_distr = end_point_distr
        self.start_time_distr = start_time_distr
        self.end_time_distr = end_time_distr

    @classmethod
    def trip_from_json(cls, json_segment):
        trip = E_Mission_Trip.trip_from_json(json_segment)
        trip.start_point_distr = json_segment.get("start_point_distr")
        trip.end_point_distr = json_segment.get("end_point_distr")
        trip.start_time_distr = json_segment.get("start_time_distr")
        trip.end_time_distr = json_segment.get("end_time_distr")
        return cls(trip._id, trip.user_id, trip.trip_id, trip.sections, trip.start_time, trip.end_time, trip.trip_start_location, trip.trip_end_location,
                   trip.alternatives, trip.perturbed_trips, trip.mode_list, trip.start_point_distr, trip.end_point_distr,
                   trip.start_time_distr, strip.end_time_distr)

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
    def __init__(self, _id, user_id, trip_id, sections, start_time, end_time, trip_start_location, trip_end_location, parent_id, cost, mode_list):
        super(self.__class__, self).__init__(_id, user_id, trip_id, sections, start_time, end_time, trip_start_location, trip_end_location)
        self.subtype = "alternative"
        self.parent_id = parent_id
        self.cost = cost
	self.mode_list = mode_list

    @classmethod
    def trip_from_json(cls, json_segment):
        trip = Trip.trip_from_json(json_segment)
        trip.parent_id = json_segment.get("parent_id")
        trip.cost = json_segment.get("cost")
        trip.mode_list = cls._init_mode_list(trip.sections)
	print "_init"
        return cls(trip._id, trip.user_id, trip.trip_id, trip.sections, trip.start_time, trip.end_time, trip.trip_start_location, trip.trip_end_location,
                   trip.parent_id, trip.cost, trip.mode_list)

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

    def save_to_db(self):
        db = get_alternatives_db()
        result = db.update({"_id": self._id},
                      {"$set": {"cost" : self.cost}},
                       upsert=False,multi=False)
        print result
        if not result["updatedExisting"]:
            self._create_new(db)

    def _create_new(self, db):
        db.insert({"_id": self._id, "user_id": self.user_id,
                "trip_id": self.trip_id, "trip_start_time": self.start_time.strftime(DATE_FORMAT),
                "trip_end_time": self.end_time.strftime(DATE_FORMAT), "trip_start_location": self.trip_start_location.maps_coordinate(), "trip_end_location": self.trip_end_location.maps_coordinate(),
                #"mode_list": self.mode_list})
		})
	for section in self.sections:
	    section.save_to_db()

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

    def savePipelineFlags(self, _id):
        db = get_trip_db()
        db.update({"_id": self._id},
                      {"$set": {"pipelineFlags" : {'alternativesStarted': self.alternativesStarted, 'alternativesFinished': self.alternativesFinished}}},
                       multi=False, upsert=False)

