""" Query modules mapping functions to their query strings
structured:

module_name { query_string: function_for_query }

"""

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

#returns the top trips for the user, defaulting to the top 10 trips
def getTopTrips(uid, options = 10):
    """ options that we can have for get top trips:
    """
    return []

#returns all trips to the user
def getAllTrips(uid):
    return []

def getRecentTrips(uid, options = 10):
    return []

def getTripsThroughMode(uid, options = 10):
    return[]

# Returns the trips that are suitable for training
# Currently this is:
# - trips that have alternatives, and
# - have not yet been included in a training set
def getTrainingTrips(uid)
    return []
