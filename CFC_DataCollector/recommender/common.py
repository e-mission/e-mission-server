from __future__ import division
import urllib2
import xml.etree.ElementTree as ET
from get_database import get_section_db
from datetime import datetime
import trip


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

def test():
	t = trip.E_Mission_Trip(False, [], 20, datetime(2013, 10, 10, 10), datetime(2013, 10, 10, 11), [122311, 1334113], [314354234, 325213525], 90809808, 903468035689)
	store_trip_in_db(t, True)