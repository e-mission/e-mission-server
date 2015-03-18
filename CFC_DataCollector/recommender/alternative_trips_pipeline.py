import get_trips
from common import store_trip_in_db
#import get_trips
from trip import E_Mission_Trip
import datetime
#import Profiles

def run_alternative_trips_pipeline(trip_iterator):

    while trip_iterator.hasNext():
        list_of_perturbed_trips = find_perturbed_trips(trip_iterator.next())
        #Take trip object and return interval with trip objects
        schedule_queries(list_of_perturbed_trips)

def find_perturbed_trips(trip, delta=2):
    to_return = [ ]
    time_delta = datetime.timedelta(minutes=delta)
    fifteen_min = datetime.timedelta(minutes=15)
    start = trip.start_time - fifteen_min
    end = trip.end_time + fifteen_min
    time = start
    while time < end:
        new_trip = E_Mission_Trip(0, 0, 0, 0, time, 0, trip.start_point, trip.end_point)
        to_return.append(new_trip)
    return to_return

def store_alternative_trips(tripObj):
    # store populated tripObj with _id (concatenated trip id and user id)


def get_alternative_trips(_id):
    # User Utility Pipeline calls this to get alternatve trips for one original trip (_id)
    


#get_user_ids(filter_queries)
#bare_trips = get_bare_trips()
#augment_trips()
