import sys
import googlemaps
import datetime
from crontab import CronTab

#usage:
#(From terminal)
#python query.py lat1,lon1 lat2,lon2

if len(sys.argv) == 4:
	curr_time = datetime.datetime.now()
	curr_month = curr_time.month
	curr_day = curr_time.day
	curr_hour =curr_time.hour
	curr_minute = curr_time.minute

	_id = sys.argv[1]
	start = sys.argv[2]
	end = sys.argv[3]
	modes = ['driving', 'walking', 'bicycling', 'transit']
	gmaps = googlemaps.Client(key='AIzaSyBEkw4PXVv_bsAdUmrFwatEyS6xLw3Bd9c')

	for mode in modes:
		directions_result = gmaps.directions(origin=start, destination=end,
	                                 mode=mode,
	                                 departure_time=datetime.datetime.now())
		for result in directions_result:
			#do something here related to saving the trip
			#not really sure how to maintain unique ids??
			gmaps_trip = directions_json_to_trip(result)
			update_perturbations(_id, gmaps_trip)

	#remove job from cron
	#TODO: make sure that you only remove the cronjob related to your current query, this will remove all cronjobs scheduled at the same time
	cron = CronTab()
	for job in cron:
		if job.month == curr_month and job.day == curr_day and job.hour == curr_hour and job.minute == curr_minute:
			cron.remove(job)
			print("Removed job!")

	trip = get_trip(_id)
	trip.getpipelineFlags().finishAlternatives()

else:
	print("Wrong number of input arguments.")