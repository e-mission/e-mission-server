# simple user utility model taking cost, time, and mode into account

from get_database import get_utility_model_db
from datetime import datetime
from common import calc_car_cost, DATE_FORMAT
from main import common as cm
from user_utility_model import UserUtilityModel
import numpy as np

class SimpleCostTimeModeModel(UserUtilityModel):
  def __init__(self, trips_with_alts):
    #print "len(trips) = %d, len(alternatives) = %d" % (len(trips), len(alternatives))
        super(SimpleCostTimeModeModel, self).__init__(trips_with_alts)

  def store_in_db(self, user_id):
    model_query = {'user_id': user_id, 'type':'user'}
    model_object = {'user_id': user_id, 'cost': self.cost, 'time': self.time, 'mode': self.mode, 'updated_at': datetime.now(), 'type':'user'} 
    get_utility_model_db().update(model_query, model_object, upsert = True)

  def save_coefficients(self):
    self.cost = self.coefficients[0][0]
    self.time = self.coefficients[0][1]
    self.mode = self.coefficients[0][2]


  def extract_features(self):
    num_features = 3
    feature_vector = np.empty((len(self.trips_with_alts * (self.num_alternatives+1)), num_features)) 
    label_vector = np.empty(len(self.trips_with_alts * (self.num_alternatives+1)))
    sample = 0
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
        return np.array([cost, time, mode])
