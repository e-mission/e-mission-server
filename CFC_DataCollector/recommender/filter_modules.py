""" Query modules mapping functions to their query strings
structured:

module_name { query_string: function_for_query }

"""
import sys
import os
import math
import datetime
import logging
sys.path.append("%s" % os.getcwd())
sys.path.append("%s/../../CFC_WebApp/" % os.getcwd())
logging.basicConfig(format='%(asctime)s:%(levelname)s:%(message)s', level=logging.DEBUG)

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
        clusterJson = c_db.find_one({"clusters":{"$exists":True}, "user": uid})
        if clusterJson is None:
            return []
        c_info = []
        x = clusterJson["clusters"].values() 
        for col in x:
                first = True
                y = [[] for _ in range(5)]
                for cluster in col:
                        if first:
                            rt = info
                            first = False
                        info = s_db.find_one({"_id":cluster})
                        appendIfPresent(y[0], info, "section_start_datetime")
                        appendIfPresent(y[1], info, "section_end_datetime")
                        appendIfPresent(y[2], info, "section_start_point")
                        appendIfPresent(y[3], info, "section_end_point")
                        appendIfPresent(y[4], info, "confirmed_mode")
                c_info.append(y, rt)
        return c_info

def appendIfPresent(list,element,key):
    if element is not None and key in element:
        list.append(element[key])
    else:
        logging.debug("not appending element %s with key %s" % (element, key))

class AlternativesNotFound(Exception):
    def __init__(self, value):
        self.value = value
    def __str__(self):
        return repr(self.value)

#returns the top trips for the user, defaulting to the top 10 trips
def getCanonicalTrips(uid, get_representative=False): # number returned isnt used
    """
        uid is a UUID object, not a string
    """
    # canonical_trip_list = []
    # x = 0
    # if route clusters return nothing, then get common routes for user
    #clusters = get_routeCluster_db().find_one({'$and':[{'user':uid},{'method':'lcs'}]})
    # c = get_routeCluster_db().find_one({'$and':[{'user':uid},{'method':'lcs'}]})

    logging.debug('UUID for canonical %s' % uid)
    info = get_clusters_info(uid)
    cluster_json_list = []
    for (cluster, rt) in info:
      json_dict = dict()
      json_dict["representative_trip"] = rt
      json_dict["start_point_distr"] = cluster[2]
      json_dict["end_point_distr"] = cluster[3]
      json_dict["start_time_distr"] = cluster[0]
      json_dict["end_time_distr"] = cluster[1]
      json_dict["confirmed_mode_list"] = cluster[4]
      cluster_json_list.append(json_dict)
    toRet = cluster_json_list
    print "About to return list %s" % toRet
    return toRet.__iter__()

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
        'get_improve': getCanonicalTrips
    },
   #Perturbation Module
   'perturbation': {},
   #Alternatives Module
   # note: uses a different collection than section_db
   'alternatives': {
       'get_alternatives': getAlternativeTrips
   }
}
