from __future__ import unicode_literals
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import
# Standard imports
from future import standard_library
standard_library.install_aliases()
from builtins import zip
from builtins import *
from builtins import object
import json
import uuid
import logging

# Our imports
from emission.core.get_database import get_utility_model_db
from emission.analysis.modelling.user_model.user_utility_model import UserUtilityModel
from emission.analysis.modelling.user_model.emissions_model import EmissionsModel
from emission.analysis.modelling.user_model.simple_cost_time_mode_model import SimpleCostTimeModeModel
import emission.core.wrapper.tripiterator as ti
import emission.core.wrapper.trip_old as t
from emission.net.ext_service.gmaps.common import get_uuid_list, get_recommender_uuid_list
import emission.analysis.modelling.user_model.alternative_trips_module as atm

class RecommendationPipeline(object):
    def get_trips_to_improve(self, user_uuid):
        # pick trips which we would like to improve
        # will make usage of canonical trip class
        # returns a list of trips implementing Trip interface, could be basic E_Mission_Trips or canonical
        # TODO: Stubbed out returning all move trips in order to allow tests to pass
        return ti.TripIterator(user_uuid, ["recommender", "get_improve"], t.Canonical_E_Mission_Trip)

    def retrieve_alternative_trips(self, trip_list):
        return []

    def get_selected_user_utility_model(self, user_id, trips):
        #return UserUtilityModel.find_from_db(user_id)
        model_json = self.find_from_db(user_id, False)
        logging.debug("for user %s, existing model is %s" % (user_id, model_json))
        if model_json:
            cost_coeff = model_json.get("cost")
            time_coeff = model_json.get("time")
            mode_coeff = model_json.get("mode")
            model2 = EmissionsModel(cost_coeff, time_coeff, mode_coeff, trips)
            logging.debug("for user %s, modified model is %s" % (user_id, model2))
            return model2
        return None

    def _evaluate_trip(self, utility_model, trip):
        return utility_model.predict_utility(trip)

    def runPipeline(self):
        for user_uuid in get_recommender_uuid_list():
            recommend_trips = []
	    # Converting to a list because otherwise, when we prepare feature
	    # vectors, the iterator is all used up
            trips_to_improve = list(self.get_trips_to_improve(user_uuid))
            alternatives = atm.get_alternative_for_trips(trips_to_improve)
            trips_with_alts = self.prepare_feature_vectors(trips_to_improve, alternatives)
            logging.debug("trips_with_alts = %s" % trips_with_alts)
            user_model = self.get_selected_user_utility_model(user_uuid, trips_to_improve)
            if user_model:
                for trip_to_improve in trips_to_improve:
                    logging.debug("user_model = %s" % user_model)
                    original_trip = trip_to_improve
                    if (len(list(atm.get_alternative_for_trip(original_trip))) == 0):
                        logging.debug("trip = %s has no alternatives, skipping..." % original_trip._id)
                        continue;
                    logging.debug("considering for recommendation, original trip = %s " % original_trip.__dict__)
                    recommended_trip = user_model.predict(original_trip)
                    logging.debug("recommended_trip = %s" % recommended_trip)
                    if original_trip != recommended_trip:
                        logging.debug("recommended trip is different, setting it")
                        original_trip.mark_recommended(recommended_trip)
                        recommend_trips.append(recommended_trip)
                    else: 
                        logging.debug("Original Trip is best")
            else:
                logging.debug("No user model found, skipping")
                
 
    def find_from_db(self, user_id, modified):
        if modified:
            db_model = get_utility_model_db().find_one({'user_id': user_id, 'type':'recommender'})
        else:
            db_model = get_utility_model_db().find_one({'user_id': user_id, 'type':'user'})
        return db_model

    def prepare_feature_vectors(self, trips, alternatives):
	logging.debug("Before zipping, trips, alternatives lengths are %d, %d " % (len(list(trips)), len(alternatives)))
        vector = list(zip(trips, alternatives))
	logging.debug("After zipping, vector length is %d " % len(vector))
        vector = [(trip,list(alts)) for trip, alts in vector if alts] 
	logging.debug("After preparing feature vectors, vector length is %d " % len(vector))
        return vector


if __name__ == "__main__":

  with open('config.json') as cf:
      config_data = json.load(cf)
  log_base_dir = config_data['paths']['log_base_dir']
  logging.basicConfig(format='%(asctime)s:%(levelname)s:%(message)s',
                      filename="%s/pipeline.log" % log_base_dir, level=logging.DEBUG)
  recommendationPipeline = RecommendationPipeline()
  recommendationPipeline.runPipeline()
