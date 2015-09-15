"""
Construct user utility model or retrieve from database and update with
augmented trips. Store in database and return the model.
"""
# Standard imports
import json
import logging
from pymongo.errors import ConnectionFailure

# Our imports
import user_utility_model
import emission.core.wrapper.tripiterator as ti
import alternative_trips_module as atm
import emission.net.ext_service.gmaps.common as egcm
import simple_cost_time_mode_model as sctm
import emission.core.wrapper.trip_old as ecwt

class UtilityModelPipeline:
    def __init__(self):
        pass

    def get_training_trips(self, user_id):
        return ti.TripIterator(user_id, ["utility", "get_training"], ecwt.E_Mission_Trip)

    def build_user_model(self, user_id, trips):
        model = user_utility_model.UserUtilityModel.find_from_db(user_id, False)
        trips = list(trips)
        logging.debug("Building user model for %s with %d trips " % (user_id, len(trips)))
        # alternatives = atm.get_alternative_trips(trips)
        # logging.debug("Number of alternatives = %d" % len(alternatives))
        # trips_with_alts = self.prepare_feature_vectors(trips, alternatives)
        #TODO: figure out json parsing for model creation
        if len(trips) > 0:
            '''
            if model:
              model.update(trips_with_alts)
            '''
            logging.info("Building Model")
            # model = SimpleCostTimeModeModel(trips_with_alts)
            try:
                model = sctm.SimpleCostTimeModeModel(trips)
                model.update()
                '''
                model2 = EmissionsModel(model.cost, model.time, model.mode, trips_with_alts)
                model2.update()
                '''
                model.store_in_db(user_id)
                #model2.store_in_db(user_id)
                return model
            except Exception, e:
                logging.info("Exception %s while building model", e)
                model = sctm.SimpleCostTimeModeModel()
                return model
        else:
            logging.info("No alternatives found")
            model = sctm.SimpleCostTimeModeModel()
            return model
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

    def runPipeline(self):
        for user_uuid in egcm.get_training_uuid_list():
            try:
                training_real_trips = self.get_training_trips(user_uuid)
                userModel = self.build_user_model(user_uuid, training_real_trips)
            except ConnectionFailure, e:
                logging.error("Found error %s!, skipping user %s" % (e, user_uuid))

if __name__ == "__main__":
  config_data = json.load(open('config.json'))
  log_base_dir = config_data['paths']['log_base_dir']
  logging.basicConfig(format='%(asctime)s:%(levelname)s:%(message)s',
                      filename="%s/pipeline.log" % log_base_dir, level=logging.DEBUG)
  utilityModelPipeline = UtilityModelPipeline()
  utilityModelPipeline.runPipeline()
