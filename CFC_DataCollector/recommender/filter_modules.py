""" Query modules mapping functions to their query strings
structured:

module_name { query_string: function_for_query }

"""
import sys
import os
import math
import datetime
sys.path.append("%s" % os.getcwd())
sys.path.append("%s/../../CFC_WebApp/" % os.getcwd())

from main.K_medoid_2 import kmedoids, user_route_data
from main.route_matching import update_user_routeDistanceMatrix, update_user_routeClusters

from get_database import get_section_db, get_trip_db, get_routeCluster_db, get_alternatives_db
import trip
import random
from uuid import UUID

from main.get_database import get_routeCluster_db, get_section_db
# 0763de67-f61e-3f5d-90e7-518e69793954
# 0763de67-f61e-3f5d-90e7-518e69793954_20150421T230304-0700_0
# helper for getCanonicalTrips
def get_clusters_info(uid):
        c_db = get_routeCluster_db()
        s_db = get_section_db()
        x = c_db.find_one({"clusters":{"$exists":True}, "user": uid})["clusters"].values()
        c_info = []
        for col in x:
                y = [[] for _ in range(5)]
                for cluster in col:
                        info = s_db.find_one({"_id":cluster})
                        y[0].append(info["section_start_datetime"])
                        y[1].append(info["section_end_datetime"])
                        y[2].append(info["section_start_point"]["coordinates"])
                        y[3].append(info["section_end_point"]["coordinates"])
                        y[4].append(info["confirmed_mode"])
                c_info += [y]
        return c_info

class AlternativesNotFound(Exception):
    def __init__(self, value):
        self.value = value
    def __str__(self):
        return repr(self.value)

#returns the top trips for the user, defaulting to the top 10 trips
def getCanonicalTrips(uid): # number returned isnt used
    # canonical_trip_list = []
    # x = 0
    # if route clusters return nothing, then get common routes for user
    #clusters = get_routeCluster_db().find_one({'$and':[{'user':uid},{'method':'lcs'}]})
    # c = get_routeCluster_db().find_one({'$and':[{'user':uid},{'method':'lcs'}]})

    info = get_clusters_info(UUID(uid))
    cluster_json_list = []
    for cluster in info:
      json_dict = dict()
      json_dict["start_point_distr"] = cluster[2]
      json_dict["end_point_distr"] = cluster[3]
      json_dict["start_time_distr"] = cluster[0]
      json_dict["end_time_distr"] = cluster[1]
      json_dict["confirmed_mode_list"] = cluster[4]
      cluster_json_list.append(json_dict)
    return [trip.Canonical_E_Mission_Trip.trip_from_json(c) for c in cluster_json_list]

    # clusters = c['clusters'] if c else [] 
    # #assert len(clusters) > 0, ("Could not get any route clusters for user with uid ", uid)
    # print get_section_db().find({"user_id": uid}).count()

    # if not clusters:
    #     print "updating route clusters"
    #     # no clusters found for user, run algorithm to populate database
    #     routes_user = user_route_data(uid,get_section_db())
    #     print "routes_user = %s" % routes_user
    #     update_user_routeDistanceMatrix(uid,routes_user,step1=100000,step2=100000,method='lcs')
    #     clusters_user = kmedoids(routes_user,int(math.ceil(len(routes_user)/8) + 1),uid,method='lcs')
    #     print "clusters_users = %s" % str(clusters_user)
    #     update_user_routeClusters(uid,clusters_user[2],method='lcs')
    #     #try getting clusters again
    #     #clusters = get_routeCluster_db().find_one({'$and':[{'user':uid},{'method':'lcs'}]})['clusters']
    #     c = get_routeCluster_db().find_one({'$and':[{'user':uid},{'method':'lcs'}]})
    #     clusters = c['clusters'] if c else [] 
    #     #assert len(clusters) > 0, ("Could not get any route clusters for user with uid ", uid)
    #     if not clusters:
    #         #TODO: returns a random ten trips right now if clusters aren't created
    #         for trip in get_section_db().find({"user_id":uid}):
    #             if x <= number_returned:
    #                 canonical_trip_list.append(trip)
    #                 x+=1

    #         return iter(canonical_trip_list)

    # # sort user route clusters to get most popular trips
    # print "After constructing clusters, list is %s" % clusters
    # sorted_clusters = sorted(clusters, key=lambda cluster_key: len(user_route_clusters[cluster_key]), reverse=True)
    # for cid in sorted_clusters:
    #     if x <= number_returned:
    #         canonical_trip_list.append(random.choice(user_route_clusters[cid]))
    #         x+=1

    # return iter(canonical_trip_list)

#returns all trips to the user
def getAllTrips(uid):
    #trips = list(get_trip_db().find({"user_id":uid, "type":"move"}))
    query = {'user_id':uid, 'type':'move'}
    return get_trip_db().find(query)

def getAllTrips_Date(uid, dys):
    #trips = list(get_trip_db().find({"user_id":uid, "type":"move"}))
    d = datetime.datetime.now() - datetime.timedelta(days=dys)
    query = {'user_id':uid, 'type':'move','trip_start_datetime':{"$gt":d}}
    return get_trip_db().find(query)

# Returns the trips that are suitable for training
# Currently this is:
# - trips that have alternatives, and
# - have not yet been included in a training set
def getTrainingTrips(uid):
    query = {'user_id':uid, 'type':'move'}
    return get_trip_db().find(query)

def getTrainingTrips_Date(uid, dys):
    d = datetime.datetime.now() - datetime.timedelta(days=dys)
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
        print alternatives.count()
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
   #Perturbation Module
   'perturbation': {},
   #Alternatives Module
   # note: uses a different collection than section_db
   'alternatives': {
       'get_alternatives': getAlternativeTrips
   }
}
