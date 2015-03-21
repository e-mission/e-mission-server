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
    return [trip.E_Mission_Trip.trip_from_json(jsonStr) for jsonStr in get_section_db().find({'user_id' : uid})].__iter__()


def trip_comparator_date(less_than):
    def compare(x, y):
        if x.start_time < y.start_time:
            return -1
        elif y.start_time < x.start_time:
            return 1
        else:
            return 0
    return compare


def getRecentTrips(uid, options = 10):
    #ts = [trip.E_Mission_Trip.trip_from_json(jsonStr) for jsonStr in get_section_db().find({"user_id" : user_id})]
    ts = getAllTrips(uid)
    sorted_list = sorted(ts, cmp=trip_comparator_date, reverse=False)
    return sorted_list[:options - 1]


def getTripsThroughMode(uid, options = 10):
    ts = getAllTrips(uid)

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

