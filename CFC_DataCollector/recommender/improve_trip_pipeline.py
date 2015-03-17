######
#1  Figure out best alternative for each trips
#1.5 For each trip do:
#3. Perturbe trips
#4. Augment perturbed trips
#5. Apply model to alternate and perturbed trips
#6. Return list of improved trips

#construction of model' can part of utility model pipeline or this pipeline

#getAugmentedTrips: retrieve augmented trips from database, 
		#determine perturbed trips,
		#combine augmented and perturbed trips (which may or may not be cached, tbd)
######
def buildRecommendedTrip(user_id, alternative_trips):
	#get improved trips
	tripsToImprove = getTripsToImprove(user_uuid, alternate_trips) 
	model = UserUtilityModel.find_from_db(user_id)
	improved_trips = []
	#for each trip
	for trip in tripsToImprove:
		alt_trips = getAugmentedTrips(trip)
		ultilized_trips = utilFunction(model, alt_trips)
		improved_trips.add(ultilized_trips)

	final_trips = sortByUtility(improved_trips)
	return final_trips
 
