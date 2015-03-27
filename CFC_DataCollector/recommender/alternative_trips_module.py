import get_trips
from common import store_trip_in_db, find_perturbed_trips, get_uuid_list
from trip import E_Mission_Trip
from get_database import get_alternative_trips_db, get_perturbed_trips_db

#import Profiles

"""

High level overivew of alternative_trips_pipeline

The main method for this pipeline is calc_perturbed_trips
We first construct a trip iterator, and look at each trip where the pipelineFlags attribte "alternativesStarted" is False
For each of these trips, we create an array of trip objects, which are modifications to the original trip object with perturbed times
We also have a database collection that associates each original trip, with all of its perturbed trips

For each array of perturbed trip objects, we schedule those queries via CRON jobs
We initialize each perturbed trip as None, and we update the collection as the queries are being made


Overview of helper files relevant to this pipeline:
    -query.py -> makes a google maps query immediately to get the directions
              -> will also store the query into the database collection
    -perturb.py -> schedules calls to query.py on cron jobs

    @TODO: this doesn't exist yet, but it would make more sense than to put it in here in order to keep the code clean
    -database_util.py -> contains all the helper methods to abstract out interaction with the database


"""


# Invoked in recommendation pipeline to get perturbed trips user should consider
def calc_perturbed_trips():
    all_users = get_uuid_list()
    for user in all_users:
        trip_iterator = TripIterator(user.id, ["trips", "get top trips", 5])
        while (trip_iterator.next() != None):
            curr_trip = trip_iterator.next()
            if not curr_trip.getPipelineFlags().alternativesStarted:
                curr_trip.getPipelineFlags().startAlternatives()
                curr_unique_id = curr_trip._id
                list_of_perturbed_trips = find_perturbed_trips(curr_unique_id)
                
                #@TODO: need a method that initializes the relationship bewteen a trip, and its perturbations
                #initialize_perturbations(curr_unique_id)
                schedule_queries(list_of_perturbed_trips)


#@TODO: put these methods in database_util.py
#@TODO: These stubs need to be passed in a unique id as well as a trip ppboject
#when we store in the database, they need to be able to associate the perturbed trips back to the original trip, 
#otherwise the query made by the utility model team will be very difficult

def store_alternative_trips(tripObj):
    # store populated tripObj with _id (concatenated trip id and user id)
    db = get_alternative_trips_db()
    _id = tripObj.get_id()
    db.insert_one({_id : tripObj})

def get_alternative_trips(_id):
    # User Utility Pipeline calls this to get alternatve trips for one original trip (_id)
    # db = get_alternative_trips_db()
    # _id = tripObj.get_id()
    # return db.find(_id)
    return [trip.E_Mission_Trip.trip_from_json(jsonStr) for jsonStr in get_alternative_trips_db().find({'_id' : _id})].__iter__()

def store_perturbed_trips(tripObj):
    # store populated tripObj with _id (concatenated trip id and user id)
    db = get_perturbed_trips_db()
    _id = tripObj.get_id()
    db.insert_one({_id : tripObj})

def get_perturbed_trips(_id):
    # User Utility Pipeline calls this to get alternatve trips for one original trip (_id)
    # db = get_perturbed_trips_db()
    # _id = tripObj.get_id()
    # return db.find(_id)
    return [trip.E_Mission_Trip.trip_from_json(jsonStr) for jsonStr in get_perturbed_trips_db().find({'_id' : _id})].__iter__()

