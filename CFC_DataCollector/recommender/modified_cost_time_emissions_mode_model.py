# simple user utility model taking cost, time, and mode into account

from get_database import get_utility_model_db
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
	emission_constant = 100
	emissions = time * emission_constant 
        features.append((cost, time, mode, emissions))

    return features

