"""
Construct user utility model or retrieve from database and update with
augmented trips. Store in database and return the model.
"""

from user_utility_model import UserUtilityModel
import tripiterator as ti
import alternative_trips_module as atm
from common import get_uuid_list
from simple_cost_time_mode_model import SimpleCostTimeModeModel
from modified_cost_time_emissions_mode_model import ModifiedCostTimeEmissionsModeModel
import logging

class UtilityModelPipeline:
    def __init__(self):
        pass

    def get_training_trips(self, user_id):
        return ti.TripIterator(user_id, ["utility", "get_training"])

    def build_user_model(self, user_id, trips):
      model = UserUtilityModel.find_from_db(user_id)
      tripList = list(trips)
#print tripList
      alternatives = []
      alternatives.append(list(atm.get_alternative_trips(tripList)))
#      print alternatives	
      #alternatives.append(atm.get_alternative_trips(trip._id))

      if model:
        model.update(tripList, alternatives)
      else:
        model = SimpleCostTimeModeModel(user_id, tripList,alternatives)
      return model

    '''
    def build_user_imp_model(self, user_id, trips):
      tripList = list(trips)
      print tripList
      alternatives = []
      alternatives.append(list(atm.get_alternative_trips(tripList)))
      print alternatives	
      model = ModifiedCost(user_id, tripList,alternatives)
      return model
    '''


    # We have two options for recommendation: adjusting user utility model,
    # incorporating factors such as emissions, or adjusting user trips,
    # perhaps finding a trip that meets the user needs better but that they haven't
    # considered.
    def modify_user_utility_model(user_id, base_model):
        return ModifiedCostTimeEmissionsModeModel(base_model)

    def runPipeline(self):
        for user_uuid in get_uuid_list():
            training_real_trips = list(self.get_training_trips(user_uuid))
            userModel = self.build_user_model(user_uuid, training_real_trips)
            # TODO: Should we store the user model or the modified user model in the DB?
            userModel.store_in_db()
            modifiedUserModel = self.modify_user_utility_model(userModel)
            alternatives = []
            #print list(atm.get_alternative_trips(training_real_trips))
            alternatives.append(list(atm.get_alternative_trips(training_real_trips)))
	    modifiedUserModel.update(list(training_real_trips), alternatives)
            modifiedUserModel.store_in_db()

if __name__ == "__main__":
  import json

  config_data = json.load(open('config.json'))
  log_base_dir = config_data['paths']['log_base_dir']
  logging.basicConfig(format='%(asctime)s:%(levelname)s:%(message)s',
                      filename="%s/pipeline.log" % log_base_dir, level=logging.DEBUG)
  utilityModelPipeline = UtilityModelPipeline()
  utilityModelPipeline.runPipeline()
