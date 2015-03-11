######
#1  Figure out best alternative for each trips
#1.5 For each trip do:
#3. Perturbe trips
#4. Augment perturbed trips
#5. Apply model to alternate and perturbed trips
#6. Return list of improved trips
######
def buildRecommendedTrip(user_uuid, alternative_trips, model):
	#get improved trips
	tripsToImprove = getTripsToImprove(user_uuid, alternate_trips) 

	improved_trips = []
	#for each trip
	for trip in tripsToImprove:
		#getAugmentedTrips: retrieve augmented trips from database, 
		#determine perturbed trip,
		#combine augmented and perturbed trips (which may or may not be cached, tbd)
		alt_trips = getAugmentedTrips(trip)
		ultilized_trips = utilFunction(model, alt_trips)
		improved_trips.add(ultilized_trips)

	final_trips = sortByUtility(improved_trips)
	return final_trips
 