""" Query modules mapping functions to their query strings
structured:

module_name { query_string: function_for_query }

"""
from get_database import get_section_db, get_routeCluster_db
import trip
import random

#returns the top trips for the user, defaulting to the top 10 trips
def getCanonicalTrips(uid, number_returned = 10):
    canonical_trip_list = []
    # if route clusters return nothing, then get common routes for user
    user_route_clusters = get_routeCluster_db().find_one({'$and':[{'user':user_id},{'method':lcs}]})['clusters']
    if user_route_clusters==None:
        # no clusters found for user, run algorithm to populate database
        routes_user = user_route_data(user,get_section_db())
        update_user_routeDistanceMatrix(user,routes_user,step1=100000,step2=100000,method='lcs')
        clusters_user = kmedoids(routes_user,int(math.ceil(len(routes_user)/8) + 1),user,method='lcs')
        update_user_routeClusters(user,clusters_user[2],method='lcs')
        #try getting clusters again
        user_route_clusters = get_routeCluster_db().find_one({'$and':[{'user':user_id},{'method':lcs}]})['clusters']
        assert user_route_clusters != None, ("Could not get any route clusters for user with uid ", uid)

    # sort user route clusters to get most popular trips
    sorted_clusters = sorted(user_route_clusters, key=lambda cluster_key: len(user_route_clusters[cluster_key]), reverse=True)
    x = 0
    for cid in sorted_clusters:
        if x <= number_returned:
            canonical_trip_list.append(random.choice(user_route_clusters[cid]))
            x+=1

    return iter(canonical_trip_list)


#returns all trips to the user
def getAllTrips(uid):
    #return [trip.E_Mission_Trip.trip_from_json(jsonStr) for jsonStr in get_section_db().find({'user_id' : uid})].__iter__()
    return trip.E_Mission_Trip(jsonStr) for jsonStr in get_section_db().find({'user_id' : uid})


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
    return []

def getTripsThroughMode(uid, options = 10):
    return[]

# Returns the trips that are suitable for training
# Currently this is:
# - trips that have alternatives, and
# - have not yet been included in a training set
def getTrainingTrips(uid):
    queryString = {'type':'move'}
    #return [trip.E_Mission_Trip.trip_from_json(jsonStr) for jsonStr in get_section_db().find(queryString)].__iter__()
    return [trip.E_Mission_Trip(jsonStr) for jsonStr in get_section_db().find(queryString)].__iter__()

def getTopAlternatives(uid, options = 10):
  return []



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

   #Alternatives Module
   # note: uses a different collection than section_db
   'alternatives': {
   'get_top': getTopAlternatives
   }
 }
