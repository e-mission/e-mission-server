from trip import E_Mission_Trip
from alternative_trips_module import calc_alternative_trips
import tripiterator as ti
from common import get_uuid_list


# How to use the pipeline:
# 1. 

def get_trips_for_alternatives(user_uuid):
    # TODO: Should this be all, or should it only be the trips that don't have alternatives yet
    return ti.TripIterator(user_uuid, ["trips", "get_all"])

for user_uuid in get_uuid_list():
    bare_trips = get_trips_for_alternatives(user_uuid)
    calc_alternative_trips(bare_trips)
