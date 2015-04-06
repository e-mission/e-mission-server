# simple user utility model taking cost, time, and mode into account

from get_database import get_utility_model_db
from datetime import datetime
from common import calc_car_cost
from main import common as cm
from user_utility_model import UserUtilityModel

class ModifiedCostTimeEmissionsModeModel(UserUtilityModel):
  def __init__(self, ctm_model):
    # TODO: Fix this at the same time as the other coefficient stuff
    # self.cost = ctm_model.cost
    # self.time = ctm_model.time
    # self.mode = ctm_model.mode
    # Let's make the emissions coef be the average of the other two to make sure that
    # is scaled in the same range
    # self.emissions = (ctm_model.cost + ctm_model.time) / 2
    self.user_id = ctm_model.user_id
    pass

  def store_in_db(self):
    model_query = {'user_id': self.user_id}
    # model_object = {'cost': self.cost, 'time': self.time, 'mode': self.mode, 'emissions': self.emissions, 'updated_at': datetime.now()}
    model_object = {}
    get_utility_model_db().update(model_query, model_object, upsert = True)

  # current features are cost, time, mode
  def extract_features(trip):
    cost = trip.cost
    time = cm.travel_time(trip.start_time, trip.end_time)  
    mode = trip.mode
    # TODO: Add emissions here
    # for each section, find the emissions and add them up at the end
    emissions = 0

    return (cost, time, mode, emissions)
