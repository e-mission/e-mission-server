import sys
sys.path.append("/home/ubuntu/jimmy/e-mission-server/CFC_DataCollector/")
import googlemaps 
import datetime
from get_database import *
from crontab import CronTab
from trip import *
from common import google_maps_to_our_trip, update_perturbations, get_perturbed_trips_db, find_perturbed_trips
#from common import get_perturbed_trips_db, json_to_trip, find_perturbed_trips, initialize_empty_perturbed_trips, update_perturbations 
import optparse
from otp import OTP

#usage:
#(From terminal)
#python query.py lat1,lon1 lat2,lon2
def commandArgs(argv):
    parser = optparse.OptionParser(description = '')
    parser.add_option('--id',
                      dest = 'id',
                      help = 'Unique ID for trip (_id)')
    (options, args) = parser.parse_args(argv)  
    if not options.id:
        raise Exception("No Trip ID given")
    return options.id

def google_maps_trips(otp_mode, trip_id):
        #modes = ['driving', 'walking', 'bicycling', 'transit']
        otp_to_google_mode = {"CAR":"driving", "WALK":"walking", "BICYCLE":"bicycling", "TRANSIT":"transit"}
        mode = otp_to_google_mode[otp_mode]
        new_id = trip_id + "_" + mode
        gmaps = googlemaps.GoogleMaps('AIzaSyBEkw4PXVv_bsAdUmrFwatEyS6xLw3Bd9c')
        result = gmaps.directions(origin=start_coord, destination=end_coord, mode=mode)
        gmaps_trip = google_maps_to_our_trip(result, new_id, trip_id, trip_id, mode, curr_time)
        gmaps_trip.save_to_db()

def write_day(month, day, year):
    return "%s-%s-%s" % (month, day, year)

def write_time(hour, minute):
    return "%s:%s" % (hour, minute) 

if __name__ == '__main__':
	trip_id = commandArgs(sys.argv)
	new_id = trip_id
	db = get_trip_db()
	trip = E_Mission_Trip.trip_from_json(db.find_one({"_id": trip_id}))
	start_coord = trip.trip_start_location.maps_coordinate()
	end_coord = trip.trip_end_location.maps_coordinate()
	print "Start: ", start_coord
	print "End: ", end_coord
	'''
	else:
	    (start_lat, start_lon, end_lat, end_lon) = argsList
 	    start_coord = str((float(start_lat), float(start_lon)))
 	    end_coord = str((float(end_lat), float(end_lon)))
	'''
	    
	curr_time = datetime.datetime.now()
	curr_month = curr_time.month
	curr_day = curr_time.day
	curr_hour = curr_time.hour
	curr_minute = curr_time.minute

	otp_modes = ['CAR', 'WALK', 'BICYCLE', 'TRANSIT']
	
        for mode in otp_modes:
                #do something here related to saving the trip
                #not really sure how to maintain unique ids??
                #new_id = str(create_trip_id()) + str(_id)
		#new_id = curr_time
		new_id = trip_id + "_" + mode
                try:
		    otp_trip = OTP(start_coord, end_coord, mode, write_day(curr_month, curr_day, "2015"), write_time(curr_hour, curr_minute), False)
     		    otp_trip = otp_trip.turn_into_trip(new_id, trip_id, trip_id) 
		    otp_trip.save_to_db()
                except KeyError as e:
                    print e, "\n\n\n"
                    google_maps_trips(mode, trip_id)

        '''

	#remove job from cronjob
	#TODO: make sure that you only remove the cronjob related to your current query, this will remove all cronjobs scheduled at the same time
	cron = CronTab()
	for job in cron:
		if job.month == curr_month and job.day == curr_day and job.hour == curr_hour and job.minute == curr_minute:
			cron.remove(job)
			print("Removed job!")

	pdb = get_perturbed_trips_db()
	trip = pdb.find_one({"_id" : _id})

	all_alts_finished = True
	for pert in find_perturbed_trips(trip):
		pert._id = pert._id.replace('.', '') 
		if [pert._id] == None:
			all_alts_finished = False

	if all_alts_finished:
		trip.getpipelineFlags().finishAlternatives()

	'''
else:
	print("Wrong number of input arguments.")
