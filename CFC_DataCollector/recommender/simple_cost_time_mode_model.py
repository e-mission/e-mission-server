# simple user utility model taking cost, time, and mode into account

from get_database import get_utility_model_db

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
    cost = trip.calc_cost()
    time = trip.calc_time()
    mode = trip.mode

    return (cost, time, mode)