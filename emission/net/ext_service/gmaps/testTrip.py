# Standard imports
import json
import random

# Our imports
import emission.core.get_database as edb
import emission.net.ext_services.gmaps.common as ec
import emission.core.wrapper.trip as et
import emission.net.ext_services.gmaps.googlemaps as sdk
import emission.analysis.results.recommendation.perturb as ep

def set_up(_id):
	#fake_alternative = open("testTrip.txt", "r")
	g = sdk.GoogleMaps('AIzaSyBEkw4PXVv_bsAdUmrFwatEyS6xLw3Bd9c')
	#fake_alternative = json.loads(fake_alternative.read())
	#t1 = google_maps_to_our_trip(g.directions('2703 Hallmark Dr Belmont CA', '2341 Ellsworth Berkeley CA', 0, 0, 0, 0)
	#t2 = google_maps_to_our_trip(g.directions('1114 Madera way Belmont CA', '2510 Bancroft st Berkeley CA', 0, 0, 0, 0)
	db = edb.get_trip_db()
	our_trip = et.E_Mission_Trip.trip_from_json(db.find_one({'trip_id':_id}))
	print "our_trip duration: " , our_trip.get_duration()
	pt = ep.find_perturbed_trips(our_trip)
	new_trip = pt[0]
	#print our_trip.trip_start_location
	#print our_trip.trip_end_location
	modes = ['driving', 'walking', 'bicycling', 'transit']
	for mode in modes:
            thing = g.directions(our_trip.trip_start_location.maps_coordinate(), our_trip.trip_end_location.maps_coordinate(), mode=mode)
            alt = ec.google_maps_to_our_trip(thing, random.randint(1,10), 0, _id, 0, our_trip.start_time)
            print type(alt)
            alt.save_to_db()
	print "alternative duration: " , alt.get_duration()

if __name__ == "__main__":
	#shankari husband trip
	#set_up("20150101T141917-0800")
	set_up("20150202T185913-0800")
