import json
from user_utility_model import UserUtilityModel
from emissions_model import EmissionsModel
from simple_cost_time_mode_model import SimpleCostTimeModeModel
import tripiterator as ti
from common import get_uuid_list, get_recommender_uuid_list
import uuid
import alternative_trips_module as atm
import logging

class RecommendationPipeline:
    def get_trips_to_improve(self, user_uuid):
        # pick trips which we would like to improve
        # will make usage of canonical trip class
        # returns a list of trips implementing Trip interface, could be basic E_Mission_Trips or canonical
        # TODO: Stubbed out returning all move trips in order to allow tests to pass
        return list(ti.TripIterator(user_uuid, ["recommender", "get_improve"]))

    def retrieve_alternative_trips(self, trip_list):
        return []

    def get_selected_user_utility_model(self, user_id, trips_with_alts):
        #return UserUtilityModel.find_from_db(user_id)
        print user_id
        model = SimpleCostTimeModeModel.find_from_db(uuid.UUID(user_id), True)
        print model
        model2 = EmissionsModel(model, trips_with_alts)
        return model2

    def recommend_trips(self, trip_id, utility_model):
        return []

    def _evaluate_trip(self, utility_model, trip):
        return utility_model.predict_utility(trip)

    def save_recommendations(self, recommended_trips):
      for recommendation in recommended_trips:
        recommendation.save_to_db()

    def runPipeline(self):
        for user_uuid in get_recommender_uuid_list():
            trips_to_improve = self.get_trips_to_improve(user_uuid)
            alternatives = atm.get_alternative_trips(trips_to_improve)
            trips_with_alts = self.prepare_feature_vectors(trips_to_improve, alternatives)
            user_model = self.get_selected_user_utility_model(user_uuid, trips_with_alts)
            for trip in trips_with_alts:
                recommended_trips = user_model.predict(trip_with_alts)
            self.save_recommendations(recommended_trips)

    def prepare_feature_vectors(self, trips, alternatives):
        vector = zip(trips, alternatives)
        vector = [(trip,alts) for trip, alts in vector if alts] 
        return vector


if __name__ == "__main__":

  config_data = json.load(open('config.json'))
  log_base_dir = config_data['paths']['log_base_dir']
  logging.basicConfig(format='%(asctime)s:%(levelname)s:%(message)s',
                      filename="%s/pipeline.log" % log_base_dir, level=logging.DEBUG)
  recommendationPipeline = RecommendationPipeline()
  recommendationPipeline.runPipeline()
