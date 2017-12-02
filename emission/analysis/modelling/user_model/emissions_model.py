from __future__ import print_function
from __future__ import absolute_import
# simple user utility model taking cost, time, and mode into account
# Standard imports
import numpy as np
import math
from datetime import datetime
import logging

# Our imports
import emission.core.get_database as edb
import emission.core.common as cm
from . import alternative_trips_module as atm
from . import user_utility_model as utm

class EmissionsModel(utm.UserUtilityModel):
  def __init__(self, cost, time, mode, trips):
    super(EmissionsModel, self).__init__(trips)
    self.feature_list = ["cost", "time", "mode", "emissions"]
    '''
    self.cost = ctm_model.cost
    self.time = ctm_model.time
    self.mode = ctm_model.mode
    '''
    self.cost = cost
    self.time = time
    self.mode = mode
    # Let's make the emissions coef be the average of the other two to make sure that
    # is scaled in the same range
    self.emissions = -100000 * ((self.cost + self.time) / 2)
    self.coefficients = [self.cost, self.time, self.mode, self.emissions]
    logging.debug("emissions weight: %s" % self.emissions)

  def store_in_db(self, user_id):
    logging.debug("storing E_Missions model")
    model_query = {'user_id': user_id, 'type':'recommender'}
    model_object = {'cost': self.cost, 'user_id':user_id, 'time': self.time, 
                    'mode': self.mode, 'emissions': self.emissions, 'updated_at': datetime.now(),'type':'recommender'}
    edb.get_utility_model_db().update(model_query, model_object, upsert = True)

  def _extract_features(self, trip):
        if hasattr(trip, "cost") and isinstance(trip.cost, int):
            #cost = trip.cost
            cost = 0 
        else:
            cost = 0 
        time = cm.travel_date_time(trip.start_time, trip.end_time)
        mode = 0 # trip.mode
        emissions = self.getEmissionForTrip(trip)
        return np.array([cost, time, mode, emissions])

  def getProfile(self):
    # is user_id a uuid?
    return edb.get_profile_db().find_one({'user_id': self.user_id})

  def predict(self, trip):
    alts = list(atm.get_alternative_for_trip(trip))
    trip_features, labels = self.extract_features_for_trips([trip])
    trip_features = np.nan_to_num(trip_features)
    trip_features[np.abs(trip_features) < .001] = 0 
    trip_features[np.abs(trip_features) > 1000000] = 0 
    logging.debug("trip_features = %s" % trip_features)
    nonzero = ~np.all(trip_features==0, axis=1)
    logging.debug("nonzero count = %d" % np.count_nonzero(nonzero))
    trip_features = trip_features[nonzero]
    labels = labels[nonzero]      
    logging.debug("len(labels) = %d, len(alts) = %d" % (len(labels), len(alts)))
    # assert(len(labels) == len(alts) + 1)

    best_trip = None
    best_utility = float("-inf")
    for i, trip_feature in enumerate(trip_features):
        utility = self.predict_utility(trip_feature)
        if utility > best_utility:
                best_trip = i 
                best_utility = utility
    if labels[best_trip] == 1:
        logging.debug("Model predicts best trip is: ORIGINAL TRIP (%d)" % best_trip)
    else: 
        logging.debug("Model predicts best trip is: Alternative TRIP (%d)" % best_trip)
    if best_trip == 0:
        # Isn't this the same as the earlier check for labels[i]?
        logging.debug("labels[best_trip] == %d" % labels[best_trip])
        return trip
    else:
        logging.debug("best_trip = %s, out of %d alternatives " % (best_trip, len(alts)))
	logging.debug("corresponding alternative = %s" % (alts[best_trip-1]))
        return alts[best_trip-1]

  # calculate the utility of trip using the model
  def predict_utility(self, trip):
    utility = sum(f * c for f, c in zip(trip, self.coefficients))
    logging.debug("%s Utility: %s" % (trip, utility))
    return utility


  # Returns Average of MPG of all cars the user drives
  def getAvgMpg(self):
    mpg_array = [23]
    # All existing profiles will be missing the 'mpg_array' field.
    # TODO: Might want to write a support script here to populate it for existing data
    # and remove this additional check
    '''
    if self.getProfile() != None and 'mpg_array' in self.getProfile():
        mpg_array = self.getProfile()['mpg_array']
    '''
    total = 0
    for mpg in mpg_array:
      total += mpg
    avg = total/len(mpg_array)
    logging.debug("Returning total = %s, len = %s, avg = %s" % (total, len(mpg_array), avg))
    return avg

  # calculates emissions for trips
  def getEmissionForTrip(self,trip):
    emissions = 0
    mode_list = trip.mode_list
    if isinstance(mode_list, int):
        print("WARNING! mode_list = %s, converting to a list with one element" % mode_list)
        mode_list = [mode_list]
    mode = mode_list[0] if mode_list else "driving"
    logging.debug("Mode: %s " % mode)
    distance = trip.get_distance()
    footprint = 0
    if mode == 'driving':
        avgMetersPerGallon = self.getAvgMpg()*1.6093
        footprint = (1/avgMetersPerGallon)*8.91
    elif mode == 'bus':
        footprint = 267.0/1609
    elif mode == 'train':
        footprint = 92.0/1609
    elif mode == 'transit':
        # assume bus
        footprint = 92.0/1609
    totalDistance = distance / 1000
    emissions = totalDistance * footprint
    return emissions
