from __future__ import division
import urllib2
from get_database import *
from datetime import datetime, timedelta
import json
from trip import *
import copy
import uuid
import xml.etree.ElementTree as ET

DATE_FORMAT = "%Y%m%dT%H%M%S-%W00" #This is a great hack thought of by Shaun
CLASS_UUIDS = {
  'b0d937d0-70ef-305e-9563-440369012b39': "Shankari's Husband",
  '0763de67-f61e-3f5d-90e7-518e69793954': "Shankari Android",
  '3a307244-ecf1-3e6e-a9a7-3aaf101b40fa': "Shankari iPhone",
  '6245318c-d337-3530-9001-6b175dab73a7': "Jeff",
  '1a65368b-888e-3e77-8f7c-1128f16da1df': "Gautham",
  '5ecc845a-dbca-376f-8fb1-577bd7b18859': "Zack",
  '6433c8cf-c4c5-3741-9144-5905379ece6e': "Jimmy",
  'cc7f2ff0-8e73-3cfa-ab4c-647ebf025e42': "Shaun",
  'f8fee20c-0f32-359d-ba75-bce97a7ac83b': "Shanthi"
}

# Helper for development
def uuid_to_name(uuid):
  return CLASS_UUIDS.get(uuid, "Unknown UUID")

def get_uuid_list():
    uuids = set()
    #uuids.add(uuid.UUID('cc7f2ff0-8e73-3cfa-ab4c-647ebf025e42'))
    uuids.add(uuid.UUID('6433c8cf-c4c5-3741-9144-5905379ece6e'))
    '''
    uuids = set()
    db = get_trip_db()
    for x in db.find():
        uuids.add(x['user_id'])
    '''
    return uuids

def get_training_uuid_list():
    uuids = set()
    #uuids.add(uuid.UUID('cc7f2ff0-8e73-3cfa-ab4c-647ebf025e42'))
    uuids.add(uuid.UUID('6433c8cf-c4c5-3741-9144-5905379ece6e'))
    '''
    db = get_trip_db()
    for x in db.find():
        uuids.add(x['user_id'])
    '''
    return uuids

def coerce_gmaps_time(time):
	lst = time.split()
	if len(lst) == 4:
		return datetime.timedelta(hours=int(lst[0]), minutes=int(lst[2]))
	elif len(lst) == 2:
		return datetime.timedelta(minutes=int(lst[0]))

def google_maps_to_our_trip(google_maps_json, _id, user_id, trip_id, mode, org_start_time):
        sections = [ ]
	time = org_start_time
        for leg in google_maps_json['routes'][0]['legs']:
		td = coerce_gmaps_time(leg['duration']['text'])	
                coords = [ ]
                for step in leg['steps']:
                    coords.append(Coordinate(step['end_location']['lat'], step['end_location']['lng']))
                distance = leg['distance']
                start_location = Coordinate(leg['start_location']['lat'], leg['start_location']['lng'])
                end_location = Coordinate(leg['end_location']['lat'], leg['end_location']['lng'])
		end_time = time + td
                section = Section(0, trip_id, distance, time, end_time, start_location, end_location, mode, mode)
                section.points = coords
                sections.append(section)
		time = end_time
        start_trip = sections[0].section_start_location
        end_trip = sections[-1].section_end_location
	#TODO: actually calculate cost
	cost = 0
	parent_id = trip_id
	mode_list = [str(mode)]
        return Alternative_Trip(_id, user_id, trip_id, sections, org_start_time, end_time, start_trip, end_trip, parent_id, cost, mode_list)

def meters_to_miles(meters):
	return meters * 0.000621371

def calc_car_cost(distance):
	ave_mpg = 25
	gallons =  meters_to_miles(distance) / ave_mpg
	price = urllib2.urlopen('http://www.fueleconomy.gov/ws/rest/fuelprices')
	xml = price.read()
	p = ET.fromstring(xml)[-1]
	return float(p.text)*gallons

'''
def find_perturbed_trips(trip, delta=2):
    to_return = []
    time_delta = timedelta(minutes=delta)
    fifteen_min = timedelta(minutes=15)
    original_delta = trip.end_time- trip.start_time
    start = trip.start_time - fifteen_min
    end = trip.end_time + fifteen_min
    time = start
    while time < end:
    	_id = str(create_trip_id()) + str(trip._id)
    	json_str = {}
    	json_str['trip_start_time'] = time.strftime(DATE_FORMAT)
    	json_str['trip_end_time'] = (time + original_delta).strftime(DATE_FORMAT)   ##Asuming the perturbed trip takes as long as the original trip
    	json_str['_id'] = _id
    	json_str['mode'] = trip.mode_list
    	json_str['track_points'] = None
        new_trip = E_Mission_Trip.trip_from_json(json_str)
        to_return.append(new_trip)
        time += time_delta
    return to_return
'''
'''
#TODO: stop saving _ids it is useless
def find_perturbed_trips(trip, delta=15, num_deltas=2):
    perturbed_trips = []
    start_time_delta = timedelta(minutes=delta)
    start = trip.start_time 
    end = trip.end_time
    #Generates the original trip as well
    for i in range(-num_deltas, num_deltas):
        perturbed_trip = copy.deepcopy(trip)
        perturbed_trip._id = perturbed_trip._id + str(create_trip_id())
        perturbed_trip.start_time += i * start_time_delta
        perturbed_trips.append(perturbed_trip)
    return perturbed_trips
'''
