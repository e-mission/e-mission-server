# simple user utility model taking cost, time, and mode into account

from get_database import get_utility_model_db
from datetime import datetime
import feature_trip_extraction
from common import calc_car_cost

class SimpleCostTimeModeModel(UserUtilityModel):
  def __init__(*args):
    super(args)

    self.cost = self.coefficients[0]
    self.time = self.coefficients[1]
    self.mode = self.coefficients[2]

  def store_in_db():
    model_query = {'user_id': self.user_id}
    model_object = {'cost': self.cost, 'time': self.time, 'mode': self.mode, 'updated_at': datetime.now()}
    get_utility_model_db().update(model_query, model_object, upsert = True)

  # current features are cost, time, mode
  def extract_features(trip):
    distance = feature_trip_extraction.getDistance(trip.start_point, trip.end_point)
    if trip.mode == "car":
      cost = calc_car_cost(trip._id, distance)
    else:
      cost = 0
    time = feature_trip_extraction.travel_time(trip.start_time, trip.end_time)  
    mode = trip.mode

    return (cost, time, mode)
