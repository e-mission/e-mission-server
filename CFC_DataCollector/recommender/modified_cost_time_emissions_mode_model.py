# simple user utility model taking cost, time, and mode into account

from get_database import get_utility_model_db, get_profile_db
from datetime import datetime
from common import calc_car_cost
from main import common as cm
from user_utility_model import UserUtilityModel

class ModifiedCostTimeEmissionsModeModel(UserUtilityModel):
  def __init__(self, ctm_model):
    # TODO: Fix this at the same time as the other coefficient stuff
    self.cost = ctm_model.cost
    self.time = ctm_model.time
    self.mode = ctm_model.mode
    self.regression = ctm_model.regression
    # Let's make the emissions coef be the average of the other two to make sure that
    # is scaled in the same range
    self.emissions = -1* ((ctm_model.cost + ctm_model.time) / 2)
    print "emissions weight: ", self.emissions
    self.user_id = ctm_model.user_id
    pass

  def store_in_db(self):
    print "storing E_Missions model"
    model_query = {'user_id': self.user_id}
    model_object = {'cost': self.cost, 'time': self.time, 'mode': self.mode, 'emissions': self.emissions, 'updated_at': datetime.now()}
    get_utility_model_db().update(model_query, model_object, upsert = True)

  # current features are cost, time, mode
  def extract_features(self, trips):
    features = []
    for trip in trips:
        # TODO: E-Mission trip does not have a "cost" feature
        cost = 0 # trip.cost
        time = 1.0 / cm.travel_date_time(trip.start_time, trip.end_time)
        #time = cm.travel_date_time(datetime.strftime(trip.start_time, DATE_FORMAT), datetime.strftime(trip.end_time, DATE_FORMAT))
        # TODO: E-Mission trip does not have a "cost" feature
        mode = 0 # trip.mode
        #ASK SHAUN, why aren't we using trip.mode
	# emission_constant = 100
	# emissions = time * emission_constant 
        emissions = self.getEmissionForTrip(trip)
        features.append((cost, time, mode, emissions))

    return features

  def getProfile(self):
    # is user_id a uuid?
    return get_profile_db().find_one({'user_id': self.user_id})

  # Returns Average of MPG of all cars the user drives
  def getAvgMpg(self):
    mpg_array = [defaultMpg]
    # All existing profiles will be missing the 'mpg_array' field.
    # TODO: Might want to write a support script here to populate it for existing data
    # and remove this additional check
    mpg_array = [0]
    if self.getProfile() != None and 'mpg_array' in self.getProfile():
        mpg_array = self.getProfile()['mpg_array']
    total = 0
    for mpg in mpg_array:
      total += mpg
    avg = total/len(mpg_array)
    print "Returning total = %s, len = %s, avg = %s" % (total, len(mpg_array), avg)
    return avg

# calculates emissions for trips
  def getEmissionForTrip(trip):
    emissions = 0
    mode = trip.mode
    distance = self.distance
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

