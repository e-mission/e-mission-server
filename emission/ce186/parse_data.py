from __future__ import division

import emission.core.our_geocoder as our_geo
import datetime

def make_average_data_time_delta(time_period, our_file, given_delta, start_time):
	"""Creates an average decible level for the time delta"""
	data = open(our_file)
	averages = [ ]
	place = 1
	total_now = 0
	num_points_per_average = int(time_period // given_delta)
	for line in data:
		print line
		num = int(line)
		total_now += num
		print "place is %s" % place
		print "num_points_per_average is %s" % num_points_per_average
		if place % num_points_per_average == 0:
			print "here"
			averages.append(total_now/num_points_per_average)
			total_now = 0
		place += 1
	return averages


print make_average_data_time_delta(5, "emission/ce186/data.csv", .5, 0)