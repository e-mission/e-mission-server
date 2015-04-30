"""
Construct user utility model or retrieve from database and update with
augmented trips. Store in database and return the model.
"""
import json
from user_utility_model import UserUtilityModel
from tripiterator import TripIterator
import alternative_trips_module as atm
from common import get_uuid_list, get_training_uuid_list
from simple_cost_time_mode_model import SimpleCostTimeModeModel
from emissions_model import EmissionsModel
import logging
from trip import *

class UtilityModelPipeline:
    def __init__(self):
        pass

    def get_training_trips(self, user_id):
        return TripIterator(user_id, ["utility", "get_training"], E_Mission_Trip)

    def build_user_model(self, user_id, trips):
        model = UserUtilityModel.find_from_db(user_id, False)
        trips = list(trips)
        alternatives = atm.get_alternative_trips(trips)
        print alternatives
        trips_with_alts = self.prepare_feature_vectors(trips, alternatives)
        #TODO: figure out json parsing for model creation
        if trips_with_alts:
            '''
            if model:
              model.update(trips_with_alts)
            '''
            print "Building Model"
            model = SimpleCostTimeModeModel(trips_with_alts)
            model.update()
            '''
            model2 = EmissionsModel(model.cost, model.time, model.mode, trips_with_alts)
            model2.update()
            '''
            model.store_in_db(user_id)
            #model2.store_in_db(user_id)
            return model
        else:
            print "No alternatives found\n\n"
            return None

    def prepare_feature_vectors(self, trips, alternatives):
        vector = zip(trips, alternatives)
        vector = [(trip,alts) for trip, alts in vector if alts]
        return vector

    '''
    def build_user_imp_model(self, user_id, trips):
      trips = list(trips)
      print trips
      alternatives = []
      alternatives.append(list(atm.get_alternative_trips(trips)))
      print alternatives	
      model = ModifiedCost(user_id, trips,alternatives)
      return model
    '''


    # We have two options for recommendation: adjusting user utility model,
    # incorporating factors such as emissions, or adjusting user trips,
    # perhaps finding a trip that meets the user needs better but that they haven't
    # considered.
    def modify_user_utility_model(user_id, base_model):
        return ModifiedCostTimeEmissionsModeModel(base_model)

    def runPipeline(self):
        for user_uuid in get_training_uuid_list():
            training_real_trips = self.get_training_trips(user_uuid)
            userModel = self.build_user_model(user_uuid, training_real_trips)

if __name__ == "__main__":
  config_data = json.load(open('config.json'))
  log_base_dir = config_data['paths']['log_base_dir']
  logging.basicConfig(format='%(asctime)s:%(levelname)s:%(message)s',
                      filename="%s/pipeline.log" % log_base_dir, level=logging.DEBUG)
  utilityModelPipeline = UtilityModelPipeline()
  utilityModelPipeline.runPipeline()
