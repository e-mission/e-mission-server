""" Query modules mapping functions to their query strings
structured:

module_name { query_string: function_for_query }

"""
import sys
import os
import math
import datetime
sys.path.append("%s" % os.getcwd())
sys.path.append("%s/../CFC_WebApp/" % os.getcwd())

from main.K_medoid_2 import kmedoids, user_route_data
from main.route_matching import update_user_routeDistanceMatrix, update_user_routeClusters

from get_database import get_section_db, get_trip_db, get_routeCluster_db, get_alternatives_db
import trip
import random

class AlternativesNotFound(Exception):
    def __init__(self, value):
        self.value = value
    def __str__(self):
        return repr(self.value)

#returns the top trips for the user, defaulting to the top 10 trips
def getCanonicalTrips(uid, number_returned = 10):
    canonical_trip_list = []
    x = 0
    # if route clusters return nothing, then get common routes for user
    #clusters = get_routeCluster_db().find_one({'$and':[{'user':uid},{'method':'lcs'}]})
    c = get_routeCluster_db().find_one({'$and':[{'user':uid},{'method':'lcs'}]})
    clusters = c['clusters'] if c else [] 
    #assert len(clusters) > 0, ("Could not get any route clusters for user with uid ", uid)
    print get_section_db().find({"user_id": uid}).count()

    if not clusters:
        print "updating route clusters"
        # no clusters found for user, run algorithm to populate database
        routes_user = user_route_data(uid,get_section_db())
        print "routes_user = %s" % routes_user
        update_user_routeDistanceMatrix(uid,routes_user,step1=100000,step2=100000,method='lcs')
        clusters_user = kmedoids(routes_user,int(math.ceil(len(routes_user)/8) + 1),uid,method='lcs')
        print "clusters_users = %s" % str(clusters_user)
        update_user_routeClusters(uid,clusters_user[2],method='lcs')
        #try getting clusters again
        #clusters = get_routeCluster_db().find_one({'$and':[{'user':uid},{'method':'lcs'}]})['clusters']
        c = get_routeCluster_db().find_one({'$and':[{'user':uid},{'method':'lcs'}]})
        clusters = c['clusters'] if c else [] 
        #assert len(clusters) > 0, ("Could not get any route clusters for user with uid ", uid)
        if not clusters:
            #TODO: returns a random ten trips right now if clusters aren't created
            for trip in get_section_db().find({"user_id":uid}):
                if x <= number_returned:
                    canonical_trip_list.append(trip)
                    x+=1

            return iter(canonical_trip_list)

    # sort user route clusters to get most popular trips
    print "After constructing clusters, list is %s" % clusters
    sorted_clusters = sorted(clusters, key=lambda cluster_key: len(user_route_clusters[cluster_key]), reverse=True)
    for cid in sorted_clusters:
        if x <= number_returned:
            canonical_trip_list.append(random.choice(user_route_clusters[cid]))
            x+=1
    return iter(canonical_trip_list)

#returns all trips to the user
def getAllTrips(uid):
    #trips = list(get_trip_db().find({"user_id":uid, "type":"move"}))
    #TODO: this code, in conjunction with common.py/get_user_id, is not returning all trips for user, but rather one trip at a time
    d = datetime.datetime.now() - datetime.timedelta(days=6)
    #query = {'_id':uid, 'type':'move','trip_start_datetime':{"$gt":d}}
    query = {'user_id':uid, 'type':'move','trip_start_datetime':{"$gt":d}}
    return get_trip_db().find(query)

# Returns the trips that are suitable for training
# Currently this is:
# - trips that have alternatives, and
# - have not yet been included in a training set
def getTrainingTrips(uid):
    d = datetime.datetime.now() - datetime.timedelta(days=6)
    query = {'user_id':uid, 'type':'move','trip_start_datetime':{"$gt":d}, "pipelineFlags":{"$exists":True}}
    #query = {'user_id':uid, 'type':'move','trip_start_datetime':{"$gt":d}}
    #print get_trip_db().find(query).count()
    return get_trip_db().find(query)

def getAlternativeTrips(trip_id):
    #TODO: clean up datetime, and queries here
    #d = datetime.datetime.now() - datetime.timedelta(days=6)
    #query = {'trip_id':trip_id, 'trip_start_datetime':{"$gt":d}}
    query = {'trip_id':trip_id}
    alternatives = get_alternatives_db().find(query)
    if alternatives.count() > 0:
        return alternatives
    raise AlternativesNotFound("No Alternatives Found")

def getRecentTrips(uid):
    raise "Not Implemented Error"

def getTripsThroughMode(uid):
    raise "Not Implemented Error"

modules = {
   # Trip Module
   'trips': {
   'get_canonical': getCanonicalTrips,
   'get_all': getAllTrips,
   'get_most_recent': getRecentTrips,
   'get_trips_by_mode': getTripsThroughMode},
   # Utility Module
   'utility': {
        'get_training': getTrainingTrips
    },
   # Recommender Module
   'recommender': {
        'get_improve': getTrainingTrips
    },
   #Pertubation Module
   'pertubation': {},
   #Alternatives Module
   # note: uses a different collection than section_db
   'alternatives': {
       'get_alternatives': getAlternativeTrips
   }
}
