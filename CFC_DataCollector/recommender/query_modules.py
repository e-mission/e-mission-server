""" Query modules mapping functions to their query strings
structured:

module_name { query_string: function_for_query }

"""
from get_database import get_section_db
import trip

#returns the top trips for the user, defaulting to the top 10 trips
def getTopTrips(uid, options = 10):
    """ options that we can have for get top trips:
    """
    return []

#returns all trips to the user
def getAllTrips(uid):
    return [trip.E_Mission_Trip.trip_from_json(jsonStr) for jsonStr in get_section_db().find()].__iter__()

def getRecentTrips(uid, options = 10):
    return []

def getTripsThroughMode(uid, options = 10):
    return[]

# Returns the trips that are suitable for training
# Currently this is:
# - trips that have alternatives, and
# - have not yet been included in a training set
def getTrainingTrips(uid):
    queryString = {'type':'move'}
    return [trip.E_Mission_Trip.trip_from_json(jsonStr) for jsonStr in get_section_db().find(queryString)].__iter__()

modules = {
   # Trip Module
   'trips': {
   'get_top': getTopTrips,
   'get_all': getAllTrips,
   'get_most_recent': getRecentTrips,
   'get_trips_by_mode': getTripsThroughMode},

   # Utility Module
   'utility': {
        'get_training': getTrainingTrips
    },

   #Pertubation Module
   'pertubation': {}
 }

