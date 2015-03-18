import get_trips
from common import store_trip_in_db
#import get_trips
from trip import E_Mission_Trip
from alternative_trips_module import calc_alternative_trips_module
#import Profiles


get_user_ids(filter_queries)
bare_trips = get_bare_trips()
calc_alternative_trips_module(bare_trips)
