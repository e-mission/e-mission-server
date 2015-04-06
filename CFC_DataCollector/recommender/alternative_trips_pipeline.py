from trip import E_Mission_Trip
from alternative_trips_module import calc_alternative_trips
import tripiterator as ti
from common import get_uuid_list

class AlternativeTripsPipeline:
    def __init__(self):
        pass

    def get_trips_for_alternatives(self, user_uuid):
        return ti.TripIterator(user_uuid, ["trips", "get_all"])

    def runPipeline(self):
        for user_uuid in get_uuid_list():
            trips_with_no_alternatives = self.get_trips_for_alternatives(user_uuid)
            calc_alternative_trips(trips_with_no_alternatives)
