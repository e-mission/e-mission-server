import sys
import googlemaps
import datetime
from get_database import *
from crontab import CronTab
from common import google_maps_to_our_trip, update_perturbations, get_perturbed_trips_db, find_perturbed_trips
#from common import get_perturbed_trips_db, json_to_trip, find_perturbed_trips, initialize_empty_perturbed_trips, update_perturbations 
import optparse

#usage:
#(From terminal)
#python query.py lat1,lon1 lat2,lon2
def commandArgs(argv):
    parser = optparse.OptionParser(description = '')
    parser.add_option('--start_lat',
                      dest = 'start_lat',
                      help = 'Starting Latitude of trip')
    parser.add_option('--start_lon',
                      dest = 'start_lon',
                      help = 'Starting Longitude of trip')
    parser.add_option('--end_lat',
                      dest = 'end_lat',
                      help = 'Ending Latitude of trip')
    parser.add_option('--end_lon',
                      dest = 'end_lon',
                      help = 'Ending Longitude of trip')
    parser.add_option('--trip_id',
                      dest = '_id',
                      help = 'Unique ID for trip (_id)')
    (options, args) = parser.parse_args(argv)  
    if options._id:
	return options._id
    if not options.start_lat:
        raise Exception("No start latitude given")
    if not options.start_lon:
        raise Exception("No start longitude given")
    if not options.end_lat:
        raise Exception("No end latitude given")
    if not options.end_lat:
        raise Exception("No end longitude given")
    return (options.start_lat, options.start_lon, options.end_lat, options.end_lon)

if __name__ == '__main__':
	argsList = commandArgs(sys.argv)
	if len(argsList) == 1:
	    trip_id = argsList[0]
	    db = get_trip_db()
	    trip = E_Mission_Trip.trip_from_json(db.find_one({"_id": trip_id}))
	    start_coord = trip.trip_start_location.maps_coordinate()
	    end_coord = trip.trip_end_location.maps_coordinate()
	else:
	    (start_lat, start_lon, end_lat, end_lon) = argsList
 	    start_coord = str((float(start_lat), float(start_lon)))
 	    end_coord = str((float(end_lat), float(end_lon)))
	    
	curr_time = datetime.datetime.now()
	curr_month = curr_time.month
	curr_day = curr_time.day
	curr_hour =curr_time.hour
	curr_minute = curr_time.minute


        modes = ['driving', 'walking', 'bicycling', 'transit']
        gmaps = googlemaps.GoogleMaps('AIzaSyBEkw4PXVv_bsAdUmrFwatEyS6xLw3Bd9c')

        for mode in modes:
                result = gmaps.directions(origin=start_coord, destination=end_coord, mode=mode)
                #do something here related to saving the trip
                #not really sure how to maintain unique ids??
                #new_id = str(create_trip_id()) + str(_id)
		new_id = curr_time
                gmaps_trip = google_maps_to_our_trip(result, new_id, new_id, new_id, mode, curr_time)
		gmaps_trip.save_to_db()

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
