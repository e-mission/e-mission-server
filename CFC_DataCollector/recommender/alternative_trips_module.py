import get_trips
from common import store_trip_in_db, find_perturbed_trips
#import get_trips
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
    -database_util.py -> contains all the helper methods to abstract out interaction with the database


"""


# Invoked in recommendation pipeline to get perturbed trips user should consider
def calc_perturbed_trips():
    #TODO: query all users
    for user in all_users:
        trip_iterator = TripIterator(user.id, ["trips", "get top trips", 5])
        nxt = trip_iterator.next()
        while (nxt != None):
            curr_trip = nxt
            if not curr_trip.getpipelineFlags().alternativesStarted:
                curr_trip.getpipelineFlags().startAlternatives()
                curr_unique_id = curr_trip._id
                list_of_perturbed_trips = find_perturbed_trips(curr_unique_id)
                initialize_perturbations(curr_unique_id)
                schedule_queries(list_of_perturbed_trips)




#this call should be made asynchronously
#if it returns true, that means the perturbation queries were finished, and we are now able to move on to the next pipeline
#if it returns false, that means the perturbation queries were not finished
def invoke_next_pipeline(_id):
    if check_all_queries_made(_id) == True:
        #invoke next pipeline - glue team please fill this in
        return True
    else:
        return False


#helper function for invoke_next_pipeline()
#checks to see if all the perturbation queries have been finished
def check_all_queries_made(_id):
    #this looks inside the alternatives collection
    perturbed_trips = query_alternatives(u_id) #looks into the database
    for perturbed_u_id in perturbed_trips:
        if perturbed_trips[perturbed_u_id] == None:
            return False
    return True


def store_alternative_trips(tripObj):
    # store populated tripObj with _id (concatenated trip id and user id)
    db = get_alternative_trips_db()
    _id = tripObj.get_id()
    json = E_Mission_Trip.trip_to_json(tripObj)
    db.insert_one({_id : json})

def get_alternative_trips(_id):
    # User Utility Pipeline calls this to get alternatve trips for one original trip (_id)
    # db = get_alternative_trips_db()
    # _id = tripObj.get_id()
    # return db.find(_id)
    return [Trip(jsonStr) for jsonStr in get_alternative_trips_db().find({'_id' : _id})].__iter__()

def store_perturbed_trips(tripObj):
    # store populated tripObj with _id (concatenated trip id and user id)
    db = get_perturbed_trips_db()
    _id = tripObj.get_id()
    json = E_Mission_Trip.trip_to_json(tripObj)
    db.insert_one({_id : json})

def get_perturbed_trips(_id):
    # User Utility Pipeline calls this to get alternatve trips for one original trip (_id)
    # db = get_perturbed_trips_db()
    # _id = tripObj.get_id()
    # return db.find(_id)
    return [Trip(jsonStr) for jsonStr in get_perturbed_trips_db().find({'_id' : _id})].__iter__()




# Invoked to find the choices user had when making their trip
#Jeff - what is this code doing? it seems to do nothing
def calc_alternative_trips(trip_iterator):
    #schedule_queries(trip_iterator) 
    pass
