import get_trips
from common import store_trip_in_db
#import get_trips
from trip import E_Mission_Trip
from alternative_trips_module import calc_alternative_trips
#import Profiles

# TODO: Somebody needs to get a list of user IDs
# get_user_ids()

def get_trips_for_alternatives(user_uuid):
    # TODO: Should this be all, or should it only be the trips that don't have alternatives yet
    return gt.TripIterator(user_uuid, ["trips", "get_all"])

bare_trips = get_trips_for_alternatives(user_uuid)
calc_alternative_trips_module(bare_trips)
