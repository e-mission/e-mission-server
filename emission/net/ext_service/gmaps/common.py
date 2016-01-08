# Standard import
from __future__ import division
import urllib2
from datetime import datetime, timedelta
import json
import copy
import uuid
import xml.etree.ElementTree as ET

# Our imports
import emission.core.get_database as edb
import emission.core.wrapper.trip_old as ewt

DATE_FORMAT = "%Y%m%dT%H%M%S-%W00" #This is a great hack thought of by Shaun

def get_uuid_list():
    return edb.get_trip_db().find({}).distinct("user_id")

def get_training_uuid_list():
    return edb.get_trip_db().find({}).distinct("user_id")

def get_recommender_uuid_list():
    return edb.get_trip_db().find({}).distinct("user_id")

def coerce_gmaps_time(time):
    lst = time.split()
    if len(lst) == 4:
        return timedelta(hours=int(lst[0]), minutes=int(lst[2]))
    elif len(lst) == 2:
        return timedelta(minutes=int(lst[0]))

def google_maps_to_our_trip_list(google_maps_json, _id, user_id, trip_id, mode, org_start_time):
    routes = []
    for i in xrange(len(google_maps_json['routes'])):
        routes.append(google_maps_to_our_trip(google_maps_json, _id, user_id, trip_id, mode, org_start_time, i))
    print "routes = %s" % routes
    return routes

def google_maps_to_our_trip(google_maps_json, _id, user_id, trip_id, mode, org_start_time, itinerary=0):
    sections = [ ]
    time = org_start_time
    for leg in google_maps_json['routes'][itinerary]['legs']:
        td = coerce_gmaps_time(leg['duration']['text']) 
        coords = [ ]
        for step in leg['steps']:
            coords.append(ewt.Coordinate(step['end_location']['lat'], step['end_location']['lng']))
        distance = leg['distance']
        start_location = ewt.Coordinate(leg['start_location']['lat'], leg['start_location']['lng'])
        end_location = ewt.Coordinate(leg['end_location']['lat'], leg['end_location']['lng'])
        end_time = time + td
        section = ewt.Section(0, user_id, trip_id, distance, "move", time, end_time, start_location, end_location, mode, mode)
        section.points = coords
        sections.append(section)
        time = end_time
        start_trip = sections[0].section_start_location
        end_trip = sections[-1].section_end_location
    #TODO: actually calculate cost
    cost = 0
    parent_id = trip_id
    mode_list = [str(mode)]
    return ewt.Alternative_Trip(_id, user_id, trip_id, sections, org_start_time, end_time, start_trip, end_trip, parent_id, cost, mode_list)

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
        new_trip = ewt.E_Mission_Trip.trip_from_json(json_str)
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
