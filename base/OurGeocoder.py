import urllib, urllib2
from recommender.trip import Coordinate
import json

class ReverseGeocode:

	def __init__(self, lat, lon):
		self.lat = lat 
		self.lon = lon
		self.address = None

	def make_url(self):
		params = {
			"lat" : self.lat, 
			"lon" : self.lon,
			"format" : "json"
		}

		query_url = "http://nominatim.openstreetmap.org/reverse?"
		encoded_params = urllib.urlencode(params)
		url = query_url + encoded_params
		return url

	def get_json(self):
		request = urllib2.Request(self.make_url())
		response = urllib2.urlopen(request)
		return json.loads(response.read())

	def get_address(self):
		if self.address is None:
			jsn = self.get_json()
			self.address = jsn["display_name"]
		return self.address

class Geocode:

	def __init__(self, address):
		self.address = address
		self.coords = None

	def make_url(self):
		params = {
			"q" : self.address,
			"format" : "json"
		}

		query_url = "http://nominatim.openstreetmap.org/search?"
		encoded_params = urllib.urlencode(params)
		url = query_url + encoded_params
		return url

	def get_json(self):
		request = urllib2.Request(self.make_url())
		response = urllib2.urlopen(request)
		return json.loads(response.read())

	def get_coords(self):
		if self.coords is None:
			jsn = self.get_json()
			lat = jsn[0]["lat"]
			lon = jsn[0]["lon"]
			self.coords = Coordinate(lat, lon)
		return self.coords

