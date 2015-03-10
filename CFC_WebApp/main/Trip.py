from __future__ import division
from dao.user import User
from googlemaps import GoogleMaps
from pymongo import MongoClient
import urllib2
import xml.etree.ElementTree as ET
API_KEY = "AIzaSyBBcTJ5g9sLrufvAE5BC_KhYZ5ecmbOUxU"
gmaps = GoogleMaps(API_KEY)
sectiondb = MongoClient().Stage_database.Stage_Sections

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

class Trip(object):
	"""
	Represents a trip class. 
	Subject to change at this point.
	"""
	def __init__(self):
		self.legs = [ ]
		self.cost = None
		self.mode = None
		self.duration = None
		self.distance = None


class Leg(object):
	"""Represents the leg of a trip"""
	def __init__(self, trip_id):
		self.trip_id = 0
		self.starting_point = 0
		self.ending_point = 0
		self.mode = None
		self.cost = 0
		self.duration = 0
		self.distance = 0
		self.dirs = gmaps.directions(self.starting_point, self.ending_point)

	def calc_cost(self):
		if self.mode == "car":
			self.cost = calc_car_cost(trip_id, self.distance)
		elif self.mode == "bus" or self.mode == "train":
			self.cost = 0 ##api call to google maps
		else:
			self.cost = 0

	def set_mode(self):
		self.mode = self.dirs['routes'][0]['mode']


