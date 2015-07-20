# simple user utility model taking cost, time, and mode into account

# Standard imports
import numpy as np
import logging
from datetime import datetime

# Our imports
import emission.core.get_database as edb
import emission.core.common as cm
import alternative_trips_module as atm

class SimpleCostTimeModeModel(user_utility_model.UserUtilityModel):
  def __init__(self, trips=None):
    #print "len(trips) = %d, len(alternatives) = %d" % (len(trips), len(alternatives))
        self.feature_list = ["cost", "time", "mode"]
        if trips is None:
            trips = []
        super(SimpleCostTimeModeModel, self).__init__(trips)

  def store_in_db(self, user_id):
    model_query = {'user_id': user_id, 'type':'user'}
    model_object = {'user_id': user_id, 'cost': self.cost, 'time': self.time, 'mode': self.mode, 'updated_at': datetime.now(), 'type':'user'} 
    edb.get_utility_model_db().update(model_query, model_object, upsert = True)

  def save_coefficients(self):
    self.cost = self.coefficients[0][0]
    self.time = self.coefficients[0][1]
    self.mode = self.coefficients[0][2]

  def _extract_features(self, trip):
        if hasattr(trip, "cost") and isinstance(trip.cost, int):
            #cost = trip.cost
            cost = 0
        else:
            cost = 0
        time = cm.travel_date_time(trip.start_time, trip.end_time)
        mode = 0 # trip.mode
        return np.array([cost, time, mode])
