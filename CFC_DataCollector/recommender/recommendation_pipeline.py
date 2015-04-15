from user_utility_model import UserUtilityModel
import tripiterator as ti
from common import get_uuid_list
import logging

class RecommendationPipeline:
    def get_trips_to_improve(self, user_uuid):
        # pick trips which we would like to improve
        # will make usage of canonical trip class
        # returns a list of trips implementing Trip interface, could be basic E_Mission_Trips or canonical
        # TODO: Stubbed out returning all move trips in order to allow tests to pass
        return list(ti.TripIterator(user_uuid, ["utility", "get_training"]))

    def retrieve_alternative_trips(self, trip_list):
        return []

    def get_selected_user_utility_model(self, user_id):
        return UserUtilityModel.find_from_db(user_id)

    def recommend_trips(self, trip_id, utility_model):
        return []

    def _evaluate_trip(self, utility_model, trip):
        return utility_model.predict_utility(trip)

    def save_recommendations(self, recommended_trips):
      for recommendation in recommended_trips:
        recommendation.save_to_db()

    def runPipeline(self):
        for user_uuid in get_uuid_list():
            trips_to_improve = self.get_trips_to_improve(user_uuid)
            alts_for_trips_to_improve = self.retrieve_alternative_trips(trips_to_improve)
            user_model = self.get_selected_user_utility_model(user_uuid)
            recommended_trips = self.recommend_trips(user_model, alts_for_trips_to_improve)
            self.save_recommendations(recommended_trips)

if __name__ == "__main__":
  import json

  config_data = json.load(open('config.json'))
  log_base_dir = config_data['paths']['log_base_dir']
  logging.basicConfig(format='%(asctime)s:%(levelname)s:%(message)s',
                      filename="%s/pipeline.log" % log_base_dir, level=logging.DEBUG)
  recommendationPipeline = RecommendationPipeline()
  recommendationPipeline.runPipeline()
