# simple user utility model taking cost, time, and mode into account
import numpy as np
import math
from get_database import get_utility_model_db, get_profile_db
from datetime import datetime
from common import calc_car_cost
from main import common as cm
from user_utility_model import UserUtilityModel

class EmissionsModel(UserUtilityModel):
  def __init__(self, cost, time, mode, trips_with_alts):
    super(EmissionsModel, self).__init__(trips_with_alts)
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
    print "emissions weight: ", self.emissions

  def store_in_db(self, user_id):
    print "storing E_Missions model"
    model_query = {'user_id': user_id, 'type':'recommender'}
    model_object = {'cost': self.cost, 'user_id':user_id, 'time': self.time, 
                    'mode': self.mode, 'emissions': self.emissions, 'updated_at': datetime.now(),'type':'recommender'}
    get_utility_model_db().update(model_query, model_object, upsert = True)

  def extract_features(self, trip=None):
    num_features = 4 
    feature_vector = np.empty((len(self.trips_with_alts * (self.num_alternatives+1)), num_features)) 
    label_vector = np.empty(len(self.trips_with_alts * (self.num_alternatives+1)))
    sample = 0 
    if trip:
        feature_vector[sample] = self._extract_features(trip[0])
        label_vector[sample] = 1 
        sample += 1
        for _alt in trip[1]:
            print _alt.__dict__
            feature_vector[sample] = self._extract_features(_alt)
            label_vector[sample] = 0 
    else:
        for trip,alt in self.trips_with_alts:
            feature_vector[sample] = self._extract_features(trip)
            label_vector[sample] = 1 
            sample += 1
            for _alt in alt:
                feature_vector[sample] = self._extract_features(_alt)
                label_vector[sample] = 0 
                sample += 1
    return (feature_vector, label_vector)    

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
    return get_profile_db().find_one({'user_id': self.user_id})

  def predict(self, trip_with_alts):
    alts = list(trip_with_alts[1])
    trip_features, labels = self.extract_features(trip_with_alts)
    trip_features[np.abs(trip_features) < .001] = 0 
    trip_features[np.abs(trip_features) > 1000000] = 0 
    best_trip = None
    best_utility = float("-inf")
    for i, trip_feature in enumerate(trip_features):
        utility = self.predict_utility(trip_feature)
        if utility > best_utility:
                best_trip = i 
                best_utility = utility
    if labels[i] == 1:
        print "Model predicts best trip is: ORIGINAL TRIP", best_trip
    else: 
        print "Model predicts best trip is: Alternative TRIP", best_trip
    if best_trip == 0:
        return trip_with_alts[best_trip]
    else:
        print best_trip
        print trip_with_alts
        return alts[best_trip-1]

  # calculate the utility of trip using the model
  def predict_utility(self, trip):
    utility = sum(f * c for f, c in zip(trip, self.coefficients))
    print trip, " Utility: ", utility 
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
    print "Returning total = %s, len = %s, avg = %s" % (total, len(mpg_array), avg)
    return avg

  # calculates emissions for trips
  def getEmissionForTrip(self,trip):
    emissions = 0
    mode_list = trip.mode_list
    mode = mode_list[0] if mode_list else "driving"
    print "Mode: ", mode
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
