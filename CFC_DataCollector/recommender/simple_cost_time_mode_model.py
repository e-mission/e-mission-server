# simple user utility model taking cost, time, and mode into account

from get_database import get_utility_model_db
from datetime import datetime
from common import calc_car_cost, DATE_FORMAT
from main import common as cm
from user_utility_model import UserUtilityModel

class SimpleCostTimeModeModel(UserUtilityModel):
  def __init__(self, user_id, trips, alternatives):
    #print "len(trips) = %d, len(alternatives) = %d" % (len(trips), len(alternatives))
    super(SimpleCostTimeModeModel, self).__init__(user_id, trips, alternatives)

    # The coefficients are not available during __init__
    # TODO: Fix me
    # self.cost = self.coefficients[0]
    # self.time = self.coefficients[1]
    # self.mode = self.coefficients[2]

  def store_in_db(self):
    self.cost = self.coefficients[0][0]
    self.time = self.coefficients[0][1]
    self.mode = self.coefficients[0][2]
    model_query = {'user_id': self.user_id}
    model_object = {'cost': self.cost, 'time': self.time, 'mode': self.mode, 'updated_at': datetime.now()} 
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
        features.append((cost, time, mode))

    return features
