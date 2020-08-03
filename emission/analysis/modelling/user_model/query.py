from __future__ import unicode_literals
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import
# Standard imports
from future import standard_library
standard_library.install_aliases()
from builtins import *
import sys
import datetime
import uuid
import logging
import crontab
import optparse
import os

# Our imports
import emission.core.get_database as edb
import emission.core.wrapper.trip_old as ecwt
import emission.net.ext_service.otp.otp as otp

def obtain_alternatives(trip_id, user_id):
	db = edb.get_trip_db()
	trip = ecwt.E_Mission_Trip.trip_from_json(db.find_one({"trip_id": trip_id, "user_id": user_id}))
	logging.debug(trip.sections)
	start_coord = trip.trip_start_location.maps_coordinate()
	end_coord = trip.trip_end_location.maps_coordinate()
	logging.debug("Start: %s " % start_coord)
	logging.debug("End: %s " % end_coord)
	    
	curr_time = datetime.datetime.now()
	curr_year = curr_time.year
	curr_month = curr_time.month
	curr_day = curr_time.day
	curr_hour = curr_time.hour
	curr_minute = curr_time.minute

	otp_modes = ['CAR', 'WALK', 'BICYCLE', 'TRANSIT']
	
        for mode in otp_modes:
                try:
                    otp_trip = otp.OTP(os.environ("OTP_SERVER")).route(start_coord, end_coord, mode, write_day(curr_month, curr_day, curr_year), write_time(curr_hour, curr_minute), False)
                    otp_trip = otp_trip.turn_into_trip(None, user_id, trip_id) 
                    otp_trip.save_to_db()
                except otp.PathNotFoundException as e:
                    logging.info("No alternatives found in OTP, saving nothing")
                        
        '''

	#remove job from cronjob
	#TODO: make sure that you only remove the cronjob related to your current query, this will remove all cronjobs scheduled at the same time
	cron = crontab.CronTab()
	for job in cron:
		if job.month == curr_month and job.day == curr_day and job.hour == curr_hour and job.minute == curr_minute:
			cron.remove(job)
			print("Removed job!")

	pdb = edb.get_perturbed_trips_db()
	trip = pdb.find_one({"_id" : _id})

	all_alts_finished = True
	for pert in find_perturbed_trips(trip):
		pert._id = pert._id.replace('.', '') 
		if [pert._id] == None:
			all_alts_finished = False

	if all_alts_finished:
		trip.getpipelineFlags().finishAlternatives()

	'''

def write_day(month, day, year):
    return "%s-%s-%s" % (month, day, year)

def write_time(hour, minute):
    return "%s:%s" % (hour, minute) 

def commandArgs(argv):
    parser = optparse.OptionParser(description = '')
    parser.add_option('--trip-id',
                      dest = 'trip_id',
                      help = 'Trip ID')
    parser.add_option('--user-id',
                      dest = 'user_id',
                      help = 'User ID')
    (options, args) = parser.parse_args(argv)  
    if not options.trip_id:
        raise Exception("No Trip ID given")
    if not options.user_id:
        raise Exception("No User ID given")
    return (options.trip_id, uuid.UUID(options.user_id))

if __name__ == '__main__':
	(trip_id, user_id) = commandArgs(sys.argv)
        obtain_alternatives(trip_id, user_id)
