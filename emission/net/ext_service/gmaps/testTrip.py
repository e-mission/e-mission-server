from get_database import *
from common import *
from trip import *
import json
import random
from googlemaps import *
from perturb import *

def set_up(_id):
	#fake_alternative = open("testTrip.txt", "r")
	g = GoogleMaps('AIzaSyBEkw4PXVv_bsAdUmrFwatEyS6xLw3Bd9c')
	#fake_alternative = json.loads(fake_alternative.read())
	#t1 = google_maps_to_our_trip(g.directions('2703 Hallmark Dr Belmont CA', '2341 Ellsworth Berkeley CA', 0, 0, 0, 0)
	#t2 = google_maps_to_our_trip(g.directions('1114 Madera way Belmont CA', '2510 Bancroft st Berkeley CA', 0, 0, 0, 0)
	db = get_trip_db()
	our_trip = E_Mission_Trip.trip_from_json(db.find_one({'trip_id':_id}))
	print "our_trip duration: " , our_trip.get_duration()
	pt = find_perturbed_trips(our_trip)
	new_trip = pt[0]
	#print our_trip.trip_start_location
	#print our_trip.trip_end_location
	modes = ['driving', 'walking', 'bicycling', 'transit']
	for mode in modes:
            thing = g.directions(our_trip.trip_start_location.maps_coordinate(), our_trip.trip_end_location.maps_coordinate(), mode=mode)
            alt = google_maps_to_our_trip(thing, random.randint(1,10), 0, _id, 0, our_trip.start_time)
            print type(alt)
            alt.save_to_db()
	print "alternative duration: " , alt.get_duration()

if __name__ == "__main__":
	#shankari husband trip
	#set_up("20150101T141917-0800")
	set_up("20150202T185913-0800")
