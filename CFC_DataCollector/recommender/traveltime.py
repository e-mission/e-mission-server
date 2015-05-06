import urllib, urllib2, datetime, json

class TravelTime:

	map_otp_modes_to_walkscore = {'WALK' : 'walk', 'BYCICLE' : 'bike', 'TRANSIT' : 'transit', 'CAR' : 'drive'}

	def __init__(self, origin, dest, otp_mode, congestion):
		self.origin = origin
		self.dest = dest
		self.mode = otp_mode
		self.congestion = congestion
		self.traveltime = None

	def build_url(self):
		params = {
			'mode' : self.map_otp_modes_to_walkscore[self.mode],
			'origin' : self.origin,
			'destination' : self.dest,
			'congestion' : self.congestion,
			'wsapikey' : '756f44b2657f0e914736e66b7fa913ad'
		}

		query_url = 'http://api2.walkscore.com/api/v1/traveltime/json?'
		encoded_params = urllib.urlencode(params)
		url = query_url + encoded_params
		return url

	def get_json(self):
		request = urllib2.Request(self.build_url())
		response = urllib2.urlopen(request)
		return json.loads(response.read())

	def get_travel_time(self):
		if self.traveltime:
			return self.traveltime
		our_json = self.get_json()
		print our_json
		self.traveltime = datetime.timedelta(seconds=float(our_json["response"]["results"][0]['travel_times'][0]['seconds']))
		return self.traveltime

if __name__ == "__main__":
	tt = TravelTime("47.649677,-122.357569", "47.646757,-122.361152", 'WALK', True)
	print tt.get_travel_time()
