import get_trips
from common import store_trip_in_db
#import get_trips
from trip import E_Mission_Trip
#import Profiles

def calc_alternative_trips(trip_iterator):

    while trip_iterator.hasNext():
        list_of_perturbed_trips = find_perturbed_trips(trip_iterator.next())
        schedule_queries(list_of_perturbed_trips)

def store_alternative_trips(tripObj):
    # store populated tripObj with _id (concatenated trip id and user id)


def get_alternative_trips(_id):
    # User Utility Pipeline calls this to get alternatve trips for one original trip (_id)
    


#get_user_ids(filter_queries)
#bare_trips = get_bare_trips()
#augment_trips()
