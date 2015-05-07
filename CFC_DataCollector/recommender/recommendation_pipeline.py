import json
from get_database import get_utility_model_db
from user_utility_model import UserUtilityModel
from emissions_model import EmissionsModel
from simple_cost_time_mode_model import SimpleCostTimeModeModel
import tripiterator as ti
import trip as t
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
        return list(ti.TripIterator(user_uuid, ["recommender", "get_improve"], trip_class=t.Canonical_E_Mission_Trip))

    def retrieve_alternative_trips(self, trip_list):
        return []

    def get_selected_user_utility_model(self, user_id, trips_with_alts):
        #return UserUtilityModel.find_from_db(user_id)
        print user_id
        model_json = self.find_from_db(user_id, False)
        if model_json:
            cost_coeff = model_json.get("cost")
            time_coeff = model_json.get("time")
            mode_coeff = model_json.get("mode")
            model2 = EmissionsModel(cost_coeff, time_coeff, mode_coeff, trips_with_alts)
            return model2
        return None

    def _evaluate_trip(self, utility_model, trip):
        return utility_model.predict_utility(trip)

    def runPipeline(self):
        for user_uuid in get_recommender_uuid_list():
            trips_to_improve = self.get_trips_to_improve(user_uuid)
            alternatives = atm.get_alternative_trips(trips_to_improve)
            trips_with_alts = self.prepare_feature_vectors(trips_to_improve, alternatives)
            user_model = self.get_selected_user_utility_model(user_uuid, trips_with_alts)
            if user_model:
                for trip_with_alts in trips_with_alts:
                    original_trip = trip_with_alts[0]
                    recommended_trip = user_model.predict(trip_with_alts)
                    print original_trip.__dict__
                    if original_trip != recommended_trip:
                        print "recommending"
                        original_trip.mark_recommended(recommended_trip)
                        recommend_trips.append(recommended_trip)
                    else: 
                        print "Original Trip is best"
                
 
    def find_from_db(self, user_id, modified):
        if modified:
            db_model = get_utility_model_db().find_one({'user_id': user_id, 'type':'recommender'})
        else:
            db_model = get_utility_model_db().find_one({'user_id': user_id, 'type':'user'})
        return db_model

    def prepare_feature_vectors(self, trips, alternatives):
        vector = zip(trips, alternatives)
        vector = [(trip,list(alts)) for trip, alts in vector if alts] 
        return vector


if __name__ == "__main__":

  config_data = json.load(open('config.json'))
  log_base_dir = config_data['paths']['log_base_dir']
  logging.basicConfig(format='%(asctime)s:%(levelname)s:%(message)s',
                      filename="%s/pipeline.log" % log_base_dir, level=logging.DEBUG)
  recommendationPipeline = RecommendationPipeline()
  recommendationPipeline.runPipeline()
