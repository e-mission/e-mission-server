"""
This script can be used to set up a Google Maps Trip Query at a specific point in time.

Necessary Google APIs:
Directions API
Distance Matrix API
Elevation API
Geocoding API
Time Zone API
Roads API

See more on how to enable these here: https://github.com/googlemaps/google-maps-services-python
"""
import googlemaps
import datetime
from crontab import CronTab
import sys


#otherwise, you can just pass in an address

python_location = sys.executable
#python_location = '/Users/jeffdh5/Desktop/e-mission/recommender/venv/bin/python'
#query_script_location = '/Users/jeffdh5/Desktop/e-mission/recommender/query.py'
query_script_location = 'recommender/query.py'

def schedule_queries(_id, trip_array):
	#_id: This is the original trip _id

	#start, end must both be represented as strings which contain the latitude and longitude of the location
	#purturbed_time must be a datetime object.
	#start/end: 'lat1,lon1'
	#example location input: '-33.8674869,151.2069902'
	#example use case:
	#schedule_query(start='-33.8674869,151.2069902', end='-33.8674869,151.2069902', time=datetime.datetime.now() + datetime.timedelta(days=10))

	#TODO: write regex to enforce that passed in locations are geocoded; i.e. lat1,lon1, lat2,lon2
	#TODO: this will run every year, need to figure out if python crontab can support yearly configuration so it only runs once

	for trip in trip_array:
		start = trip.get_start_coordinates()
		end = trip.get_end_coordinates()
		time = trip.get_time()

		cron = CronTab()
		exec_str = python_location + ' ' + query_script_location + ' ' + _id
		job = cron.new(command=exec_str)

		job.month.on(perturbed_time.month)
		job.day.on(perturbed_time.day)
		job.hour.on(perturbed_time.hour)
		job.minute.on(perturbed_time.minute)

		job.enable()
		cron.write()
		print("You have successfully scheduled this CRON job.")

# #Unit Test
# home = '37.199024,-121.831479'
# destination = '37.862591,-122.261784'
# test_time = datetime.datetime.now() + datetime.timedelta(minutes = 1)


# print("Scheduling query for: \nhome=" + home + "\ndestination=" + destination + "\ntime=" + test_time.strftime("%A %d. %B %Y"))
# schedule_query(home, destination, test_time)
