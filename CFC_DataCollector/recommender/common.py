from __future__ import division
import urllib2
import xml.etree.ElementTree as ET
from get_database import *
from datetime import datetime, timedelta
import random
import json
import jsonpickle

DATE_FORMAT = "%Y%m%dT%H%M%S-0700"
class Coordinate:
    def __init__(self, lat, lon):
        self.lat = lat
        self.lon = lon

    def get_lat(self):
        return self.lat

    def get_lon(self):
        return self.lon

from trip import E_Mission_Trip
def get_uuid_list():
	uuid_list = [ ]
	db = get_section_db()
	for x in db.find():
   		uuid_list.append(x['_id'])
	return uuid_list
'''
def insert_into_pdb(pdb, my_id, new_perturbed_trip):
	my_id = str(my_id)
	assert(type(my_id) == str)
	new_id = my_id.replace('.', "")
	assert(type(new_id) == str)
	if '.' not in new_id:
		pdb.insert({new_id : new_perturbed_trip})
'''
    
def initialize_empty_perturbed_trips(_id, pdb):
	db = get_section_db()
	json_trip = db.find_one({"_id" : _id})
	new_perturbed_trip = { }
	trip = E_Mission_Trip(json_trip)
	for pert in find_perturbed_trips(trip):
		pert._id = pert._id.replace('.', '') 
		new_perturbed_trip[pert._id] = None
	#insert_into_pdb(pdb, trip._id, new_perturbed_trip)
	_id = _id.replace('.', "")
	to_insert = { }
	to_insert['_id'] = _id
	to_insert['trips'] = new_perturbed_trip
	pdb.insert({'our_id': _id, "trips" : new_perturbed_trip})


def update_perturbations(_id, perturbed_trip):
	db = get_perturbed_trips_db()
	_id = _id.replace('.', '')
	json_trip = db.find_one({"our_id" : _id})
	json_trip[perturbed_trip._id] = jsonpickle.encode(perturbed_trip)

#def query_perturbed_trips()


def meters_to_miles(meters):
	return meters * 0.000621371

def calc_car_cost(trip_id, distance):
	uuid = sectiondb.find_one({'trip_id': trip_id})['user_id']
	our_user = User.fromUUID(uuid)
	ave_mpg = our_user.getAvgMpg()
	gallons =  meters_to_miles(distance) / ave_mpg
	price = urllib2.urlopen('http://www.fueleconomy.gov/ws/rest/fuelprices')
	xml = price.read()
	p = ET.fromstring(xml)[-1]
	return float(p.text)*gallons

def create_trip_id():
    return random.randint(100,999)

def find_perturbed_trips(trip, delta=2):
    to_return = [ ]
    time_delta = timedelta(minutes=delta)
    fifteen_min = timedelta(minutes=15)
    original_delta = trip.trip_end_time - trip.trip_start_time
    start = trip.trip_start_time - fifteen_min
    end = trip.trip_end_time + fifteen_min
    time = start
    while time < end:
    	_id = str(create_trip_id()) + str(trip._id) 
    	json_str = {}
    	json_str['trip_start_time'] = datetime.strftime(time, DATE_FORMAT)
    	json_str['trip_end_time'] = datetime.strftime(time + original_delta, DATE_FORMAT)   ##Asuming the perturbed trip takes as long as the original trip
    	json_str['_id'] = _id 
    	json_str['mode'] = trip.single_mode
    	json_str['track_points'] = None
        new_trip = E_Mission_Trip(json_str)
        to_return.append(new_trip)
        time += time_delta
    return to_return


def test_pt():
	trip = E_Mission_Trip(12, True, [], 0, datetime(2015, 10, 10, 0), datetime(2015, 10, 10, 20), 29292, 29292)
	print find_perturbed_trips(trip)
