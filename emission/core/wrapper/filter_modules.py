""" Query modules mapping functions to their query strings
structured:

module_name { query_string: function_for_query }

"""
from __future__ import absolute_import
from __future__ import unicode_literals
from __future__ import print_function
from __future__ import division
# Standard imports
from future import standard_library
standard_library.install_aliases()
from builtins import range
from builtins import *
import sys
import os
import math
import datetime
import logging
# logging.basicConfig(format='%(asctime)s:%(levelname)s:%(message)s', level=logging.DEBUG)
import random
from uuid import UUID

# Our imports
from emission.core.get_database import get_section_db, get_trip_db, get_routeCluster_db, get_alternatives_db
from . import trip_old as trip

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
        clusterSectionLists= list(clusterJson["clusters"].values()) 
	logging.debug( "Number of section lists for user %s is %s" % (uid, len(clusterSectionLists)))
        for sectionList in clusterSectionLists:
                first = True
		logging.debug( "Number of sections in sectionList for user %s is %s" % (uid, len(sectionList)))
		if (len(sectionList) == 0):
                    # There's no point in returning this cluster, let's move on
                    continue
                distributionArrays = [[] for _ in range(5)]
                for section in sectionList:
                        section_json = s_db.find_one({"_id":section})
                        if first:
                            representative_trip = section_json
                            first = False
                        appendIfPresent(distributionArrays[0], section_json, "section_start_datetime")
                        appendIfPresent(distributionArrays[1], section_json, "section_end_datetime")
                        appendIfPresent(distributionArrays[2], section_json, "section_start_point")
                        appendIfPresent(distributionArrays[3], section_json, "section_end_point")
                        appendIfPresent(distributionArrays[4], section_json, "confirmed_mode")
                c_info.append((distributionArrays, representative_trip))
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

#returns all trips with no alternatives to the user
def getNoAlternatives(uid):
    # If pipelineFlags exists then we have started alternatives, and so have
    # already scheduled the query. No need to reschedule unless the query fails.
    # TODO: If the query fails, then remove the pipelineFlags so that we will
    # reschedule.
    query = {'user_id':uid, 'type':'move', 'pipelineFlags': {'$exists': False}}
    return get_trip_db().find(query)

def getNoAlternativesPastMonth(uid):
    d = datetime.datetime.now() - datetime.timedelta(days=30)
    query = {'user_id':uid, 'type':'move', 
		'trip_start_datetime':{"$gt":d},
		'pipelineFlags': {'$exists': False}}
    return get_trip_db().find(query)

# Returns the trips that are suitable for training
# Currently this is:
# - trips that have alternatives, and
# - have not yet been included in a training set
def getTrainingTrips(uid):
    return getTrainingTrips_Date(uid, 30)
    query = {'user_id':uid, 'type':'move'}
    return get_trip_db().find(query)

def getTrainingTrips_Date(uid, dys):
    d = datetime.datetime.now() - datetime.timedelta(days=dys)
    query = {'user_id':uid, 'type':'move','trip_start_datetime':{"$gt":d}, "pipelineFlags":{"$exists":True}}
    #query = {'user_id':uid, 'type':'move','trip_start_datetime':{"$gt":d}}
    #print get_trip_db().count_documents(query)
    return get_trip_db().find(query)

def getAlternativeTrips(trip_id):
    #TODO: clean up datetime, and queries here
    #d = datetime.datetime.now() - datetime.timedelta(days=6)
    #query = {'trip_id':trip_id, 'trip_start_datetime':{"$gt":d}}
    query = {'trip_id':trip_id}
    alternatives = get_alternatives_db().find(query)
    if alternatives.estimated_document_count() > 0:
        logging.debug("Number of alternatives for trip %s is %d" % (trip_id, alternatives.estimated_document_count()))
        return alternatives
    raise AlternativesNotFound("No Alternatives Found")

def getRecentTrips(uid):
    raise NotImplementedError()

def getTripsThroughMode(uid):
    raise NotImplementedError()

modules = {
   # Trip Module
   'trips': {
   'get_canonical': getCanonicalTrips,
   'get_all': getAllTrips,
   'get_no_alternatives': getNoAlternatives,
   'get_no_alternatives_past_month': getNoAlternativesPastMonth,
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
