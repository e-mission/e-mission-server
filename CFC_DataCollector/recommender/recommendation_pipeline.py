import tripiterator as ti
from common import get_uuid_list

class RecommendationPipeline:
    def get_trips_to_improve(self, user_uuid):
        #pick trips which we would like to improve
        #will make usage of canonical trip class
        #returns a list of trips implementing Trip interface, could be basic E_Mission_Trips or canonical
        # TODO: Stubbed out returning all move trips in order to allow tests to pass
        return list(ti.TripIterator(user_uuid, ["utility", "get_training"]))

    def retrieve_alternative_trips(self, trip_list):
        return []

    def get_selected_user_utility_model(self, user_id):
        return []

    def recommend_trips(self, trip_id, utility_model):
        return []

    def _evaluate_trip(self, utility_model, trip):
        #return an integer value for the score of a trip, based on a utility model
        return 0

    def save_recommendations(self, recommendedTrips):
        pass

    def runPipeline(self):
        for user_uuid in get_uuid_list():
            trips_to_improve = self.get_trips_to_improve(user_uuid)
            alternatives_for_trips_to_improve = self.retrieve_alternative_trips(
                trips_to_improve)
            sel_user_model = self.get_selected_user_utility_model(user_uuid)
            recommended_trips = \
                self.recommend_trips(sel_user_model, alternatives_for_trips_to_improve)
            self.save_recommendations(recommended_trips)


