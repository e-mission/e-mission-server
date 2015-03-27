from __future__ import division
import urllib2
import xml.etree.ElementTree as ET
from get_database import *
from datetime import datetime, timedelta
import trip
from trip import E_Mission_Trip
import random
import json

def get_uuid_list():
	uuid_list = [ ]
	db = get_section_db()
	for x in db.find():
   		uuid_list.append(x['_id'])
	return uuid_list
    
def initialize_empty_perturbed_trips(_id):
	# Assuming I have the methods json_to_trip and trip_to_json
	db = get_section_db()
	pdb = get_perturbed_trips_db()
	json_trip = db.find_one({"_id" : _id})
	new_perturbed_trip = { }
	trip = json_to_trip(json_trip)
	for pert in find_perturbed_trips(trip):
		new_perturbed_trip[pert.id] = None
	pdb[trip._id] = new_perturbed_trip
	return


def update_perturbations(_id, perturbed_trip):
	## Assuming I have trip_to_json function
	db = get_perturbed_trips_db()
	json_trip = db.find_one({"_id" : _id})
	json_trip[perturbed_trip._id] = trip_to_json(perturbed_trip)


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

def store_trip_in_db(trip, is_alternate):
	""" Stores a trip in the databse, specify whether or not the trip is an alternative """
	db = get_section_db()
	to_insert = { }
	to_insert['user_id'] = trip.user_id
	#to_insert['distance'] = trip.distance
	to_insert['section_start_datetime'] = trip.start_time
	to_insert['section_end_datetime'] = trip.end_time
	to_insert['section_start_point'] = {'type' : 'Point', 'coordinates' : trip.start_point}
	to_insert['section_end_point'] = {'type' : 'Point', 'coordinates' : trip.end_point}
	to_insert['cost'] = trip.cost
	to_insert['single_mode'] = str(single_mode)
	to_insert['legs'] = [ ]
	for leg in trip.legs:
		l = { }
		l['mode'] = leg.mode
		l['cost'] = leg.cost
		l['duration'] = leg.duration
		l['distance'] = distance
		l['start_point'] = {'type' : 'Point', 'coordinates' : leg.starting_point}
		l['end_point'] = {'type' : 'Point', 'coordinates' : leg.ending_point}
		to_insert['legs'].append(l)
	if is_alternate:
		t = db.find_one({'trip_id' : trip.parent_tid})
		try:
			t['alternate_trips'].append(to_insert)
		except:
			t['alternate_trips'] = []
			t['alternate_trips'].append(to_insert)
	else:
		db.insert(to_insert)


# def trip_to_json(tripObj):
# 	to_insert = { }
# 	to_insert['user_id'] = trip.user_id
# 	#to_insert['distance'] = trip.distance
# 	to_insert['section_start_datetime'] = trip.start_time
# 	to_insert['section_end_datetime'] = trip.end_time
# 	to_insert['section_start_point'] = {'type' : 'Point', 'coordinates' : trip.start_point}
# 	to_insert['section_end_point'] = {'type' : 'Point', 'coordinates' : trip.end_point}
# 	to_insert['cost'] = trip.cost
# 	to_insert['single_mode'] = str(single_mode)
# 	to_insert['legs'] = [ ]
# 	for leg in trip.legs:
# 		l = { }
# 		l['mode'] = leg.mode
# 		l['cost'] = leg.cost
# 		l['duration'] = leg.duration
# 		l['distance'] = distance
# 		l['start_point'] = {'type' : 'Point', 'coordinates' : leg.starting_point}
# 		l['end_point'] = {'type' : 'Point', 'coordinates' : leg.ending_point}
# 		to_insert['legs'].append(l)
# 	final = {tripObj._id : to_insert}
# 	final_json = 

def create_trip_id():
	return random.randint(100, 999)

def test():
	t = trip.E_Mission_Trip(False, [], 20, datetime(2013, 10, 10, 10), datetime(2013, 10, 10, 11), [122311, 1334113], [314354234, 325213525], 90809808, 903468035689)
	store_trip_in_db(t, True)


def find_perturbed_trips(trip, delta=2):
    to_return = [ ]
    time_delta = timedelta(minutes=delta)
    fifteen_min = timedelta(minutes=15)
    start = trip.start_time - fifteen_min
    end = trip.end_time + fifteen_min
    time = start
    while time < end:
    	_id = int(str(create_trip_id) + str(trip._id)) 
        new_trip = E_Mission_Trip(_id, 0, 0, 0, 0, time, 0, trip.start_point, trip.end_point)
        to_return.append(new_trip)
        time += time_delta
    return to_return


def test_pt():
	trip = E_Mission_Trip(12, True, [], 0, datetime(2015, 10, 10, 0), datetime(2015, 10, 10, 20), 29292, 29292)
	print find_perturbed_trips(trip)
