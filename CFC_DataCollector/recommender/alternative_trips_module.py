import get_trips
from common import store_trip_in_db, find_perturbed_trips
#import get_trips
from trip import E_Mission_Trip

#import Profiles

# Invoked to find the choices user had when making their trip
def calc_alternative_trips(trip_iterator):
    schedule_queries(trip_iterator)

# Invoked in recommendation pipeline to get perturbed trips user should consider
def calc_perturbed_trips(trip_iterator):
    while (trip_iterator.next() != None):
        list_of_perturbed_trips = find_perturbed_trips(trip_iterator.next())
        schedule_queries(list_of_perturbed_trips)

def store_alternative_trips(tripObj):
    # store populated tripObj with _id (concatenated trip id and user id)
    db = get_alternative_trips_db()
    _id = tripObj.get_id()
    db.insert_one({_id : tripObj})

def get_alternative_trips(_id):
    # User Utility Pipeline calls this to get alternatve trips for one original trip (_id)
    db = get_alternative_trips_db()
    _id = tripObj.get_id()
    return db.find(_id)

def store_perturbed_trips(tripObj):
    # store populated tripObj with _id (concatenated trip id and user id)
    db = get_perturbed_trips_db()
    _id = tripObj.get_id()
    db.insert_one({_id : tripObj})

def get_perturbed_trips(_id):
    # User Utility Pipeline calls this to get alternatve trips for one original trip (_id)
    db = get_perturbed_trips_db()
    _id = tripObj.get_id()
    return db.find(_id)

