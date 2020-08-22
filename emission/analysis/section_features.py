from __future__ import division
from __future__ import unicode_literals
from __future__ import print_function
from __future__ import absolute_import
# Standard imports
from future import standard_library
standard_library.install_aliases()
from builtins import *
from past.utils import old_div
import math
import logging
import numpy as np
import utm
from sklearn.cluster import DBSCAN

# Our imports
from emission.core.get_database import get_section_db, get_mode_db, get_routeCluster_db,get_transit_db
from emission.core.common import calDistance, Include_place_2
from emission.analysis.modelling.tour_model.trajectory_matching.route_matching import getRoute,fullMatchDistance,matchTransitRoutes,matchTransitStops
import emission.storage.timeseries.abstract_timeseries as esta
import emission.storage.decorations.analysis_timeseries_queries as esda
import emission.core.wrapper.entry as ecwe
import emission.core.wrapper.modeprediction as ecwm
import emission.storage.decorations.trip_queries as esdt

from uuid import UUID 

Sections = get_section_db()
Modes = get_mode_db()


# The speed is in m/s
def calOverallSectionSpeed(section):
  distanceDelta = section.distance
  timeDelta = section.duration
  if timeDelta != 0:
    retVal = distanceDelta / timeDelta
  else:
    retVal = None
  logging.debug("while calculating overall section speed distanceDelta = %s, timeDelta = %s, result = %s" %
        (distanceDelta, timeDelta, retVal))
  return retVal

def calSpeed(point1, point2):
  from dateutil import parser
  distanceDelta = calDistance(point1['data']['loc']['coordinates'],
                              point2['data']['loc']['coordinates'])
  timeDelta = point2['data']['ts'] - point1['data']['ts']
  # logging.debug("while calculating speed form %s -> %s, distanceDelta = %s, timeDelta = %s" %
  #               (trackpoint1, trackpoint2, distanceDelta, timeDelta))
  if timeDelta != 0:
    return old_div(distanceDelta, timeDelta.total_seconds())
  else:
    return None

# This formula is from:
# http://www.movable-type.co.uk/scripts/latlong.html
# It returns the heading between two points using 
def calHeading(point1, point2):
    # points are in GeoJSON format, ie (lng, lat)
    phi1 = math.radians(point1[1])
    phi2 = math.radians(point2[1])
    lambda1 = math.radians(point1[0])
    lambda2 = math.radians(point2[0])

    y = math.sin(lambda2-lambda1) * math.cos(phi2)
    x = math.cos(phi1)*math.sin(phi2) - \
        math.sin(phi1)*math.cos(phi2)*math.cos(lambda2-lambda1)
    brng = math.degrees(math.atan2(y, x))
    return brng

def calHC(point1, point2, point3):
    HC = calHeading(point2, point3) - calHeading(point1, point2)
    return HC

def calHCR(section_entry):
    section = section_entry.data

    ts = esta.TimeSeries.get_time_series(section_entry.user_id)
    tq = esda.get_time_query_for_trip_like_object(section)
    locations = list(ts.find_entries(["analysis/recreated_location"], tq))

    if len(locations) < 3:
        return 0

    HCNum = 0
    for (i, point) in enumerate(locations[:-2]):
        currPoint = point
        nextPoint = locations[i+1]
        nexNextPt = locations[i+2]
        
        HC = calHC(currPoint['data']['loc']['coordinates'], nextPoint['data']['loc']['coordinates'], \
                   nexNextPt['data']['loc']['coordinates'])
        if HC >= 15:
            HCNum += 1

    sectionDist = section.distance
    if sectionDist!= None and sectionDist != 0:
        HCR = HCNum/sectionDist
        return HCR
    else:
        return 0

def calSR(section):
    if 'speeds' not in section.data:
        return 0
    speeds = section.data["speeds"]
    if len(speeds) < 2:
        return 0
    else:
        stopNum = 0
        for (i, speed) in enumerate(speeds[:-1]):
            currVelocity = speed
            if currVelocity != None and currVelocity <= 0.75:
                stopNum += 1

        sectionDist = section.data.distance
        if sectionDist != None and sectionDist != 0:
            return stopNum/sectionDist
        else:
            return 0

def calVCR(section_entry):
    section = section_entry.data
    speeds = section['speeds']
    if len(speeds) < 3:
        return 0
    else:
        Pv = 0
        for (i, speed) in enumerate(speeds[:-1]):
            velocity1 = speed
            velocity2 = speeds[i+1]
            if velocity1 != None and velocity2 != None:
                if velocity1 != 0:
                    VC = abs(velocity2 - velocity1)/velocity1
                else:
                    VC = 0
            else:
                VC = 0

            if VC > 0.7:
                Pv += 1

        sectionDist = section.distance
        if sectionDist != None and sectionDist != 0:
            return Pv/sectionDist
        else:
            return 0

def calSpeeds(section):
    try:
        return section["speeds"]
    except KeyError:
        return None

# In order to calculate the acceleration, we do the following.
# point0: (loc0, t0), point1: (loc1, t1), point2: (loc2, t2), point3: (loc3, t3)
# becomes
# speed0: ((loc1 - loc0) / (t1 - t0)), speed1: ((loc2 - loc1) / (t2-t1)),
# speed2: ((loc3 - loc2) / (t3 - t2)
# becomes
# section0: speed0 / (t1 - t0), section1: (speed1 - speed0)/(t2-t1),
# section2: (speed2 - speed1) / (t3-t2)

def calAccels(section):
  speeds = calSpeeds(section)
 
  if speeds is None or len(speeds) == 0:
    return None

  accel = np.zeros(len(speeds))
  prevSpeed = 0
  for (i, speed) in enumerate(speeds):
    currSpeed = speed # speed0
    speedDelta = currSpeed - prevSpeed # (speed0 - 0)

    # since we are working with cleaned sections, that have resampled data,
    # we know that the difference between the timestamps is 30 sec
    # and we don't need to query to determine what it actually is
    # if we ever revisit the resampling, we need to fix this again
    timeDelta = 30
    logging.debug("while calculating accels at index %d, speedDelta = %s, timeDelta = %s" %
        (i, speedDelta, timeDelta))
    if timeDelta != 0:
      accel[i] = speedDelta/timeDelta
      logging.debug("resulting acceleration is %s" % accel[i])
    # else: remains at zero
    prevSpeed = currSpeed
  return accel

def getIthMaxSpeed(section, i):
  # python does not appear to have a built-in mechanism for returning the top
  # ith max. We would need to write our own, possibly by sorting. Since it is
  # not clear whether we ever actually need this (the paper does not explain
  # which i they used), we just return the max.
  assert(i == 1)
  speeds = calSpeeds(section)
  return np.amax(speeds)

def getIthMaxAccel(section, i):
  # python does not appear to have a built-in mechanism for returning the top
  # ith max. We would need to write our own, possibly by sorting. Since it is
  # not clear whether we ever actually need this (the paper does not explain
  # which i they used), we just return the max.
  assert(i == 1)
  accels = calAccels(section)
  return np.amax(accels)

def calSpeedDistParams(speeds):
  return (np.mean(speeds), np.std(speeds))

# def user_tran_mat(user):
#     user_sections=[]
#     # print(tran_mat)
#     query = {"$and": [{'type': 'move'},{'user_id':user},\
#                       {'$or': [{'confirmed_mode':1}, {'confirmed_mode':3},\
#                                {'confirmed_mode':5},{'confirmed_mode':6},{'confirmed_mode':7}]}]}
#     # print(Sections.count_documents(query))
#     for section in Sections.find(query).sort("section_start_datetime",1):
#         user_sections.append(section)
#     if Sections.count_documents(query)>=2:
#         tran_mat=np.zeros([Modes.estimated_document_count(), Modes.estimated_document_count()])
#         for i in range(len(user_sections)-1):
#             if (user_sections[i+1]['section_start_datetime']-user_sections[i]['section_end_datetime']).seconds<=60:
#                 # print(user_sections[i+1]['section_start_datetime'],user_sections[i]['section_end_datetime'])
#                 fore_mode=user_sections[i]["confirmed_mode"]
#                 after_mode=user_sections[i+1]["confirmed_mode"]
#                 tran_mat[fore_mode-1,after_mode-1]+=1
#         row_sums = tran_mat.sum(axis=1)
#         new_mat = tran_mat / row_sums[:, np.newaxis]
#         return new_mat
#     else:
#         return None
#
# # all model
# def all_tran_mat():
#     tran_mat=np.zeros([Modes.estimated_document_count(), Modes.estimated_document_count()])
#     for user in Sections.distinct("user_id"):
#         user_sections=[]
#         # print(tran_mat)
#         query = {"$and": [{'type': 'move'},{'user_id':user},\
#                           {'$or': [{'confirmed_mode':1}, {'confirmed_mode':3},\
#                                    {'confirmed_mode':5},{'confirmed_mode':6},{'confirmed_mode':7}]}]}
#         # print(Sections.count_documents(query))
#         for section in Sections.find(query).sort("section_start_datetime",1):
#             user_sections.append(section)
#         if Sections.count_documents(query)>=2:
#             for i in range(len(user_sections)-1):
#                 if (user_sections[i+1]['section_start_datetime']-user_sections[i]['section_end_datetime']).seconds<=60:
#                     # print(user_sections[i+1]['section_start_datetime'],user_sections[i]['section_end_datetime'])
#                     fore_mode=user_sections[i]["confirmed_mode"]
#                     after_mode=user_sections[i+1]["confirmed_mode"]
#                     tran_mat[fore_mode-1,after_mode-1]+=1
#     row_sums = tran_mat.sum(axis=1)
#     new_mat = tran_mat / row_sums[:, np.newaxis]
#     return new_mat

def mode_cluster(mode,eps,sam):
    mode_change_pnts=[]
    query = {'confirmed_mode':mode}
    logging.debug("Trying to find cluster locations for %s trips" % (Sections.count_documents(query)))
    for section in Sections.find(query).sort("section_start_datetime",1):
        try:
            mode_change_pnts.append(section['section_start_point']['coordinates'])
            mode_change_pnts.append(section['section_end_point']['coordinates'])
        except:
            logging.warning("Found trip %s with missing start and/or end points" % (section['_id']))
            pass
    
    if len(mode_change_pnts) == 0:
      logging.debug("No points found in cluster input, nothing to fit..")
      return np.zeros(0)

    if len(mode_change_pnts)>=1:
        # print(mode_change_pnts)
        np_points=np.array(mode_change_pnts)
        # print(np_points[:,0])
        # fig, axes = plt.subplots(1, 1)
        # axes.scatter(np_points[:,0], np_points[:,1])
        # plt.show()
    else:
        pass
    utm_x = []
    utm_y = []
    for row in mode_change_pnts:
        # GEOJSON order is lng, lat
        utm_loc = utm.from_latlon(row[1],row[0])
        utm_x = np.append(utm_x,utm_loc[0])
        utm_y = np.append(utm_y,utm_loc[1])
    utm_location = np.column_stack((utm_x,utm_y))
    db = DBSCAN(eps=eps,min_samples=sam)
    db_fit = db.fit(utm_location)
    db_labels = db_fit.labels_
    #print db_labels
    new_db_labels = db_labels[db_labels!=-1]
    new_location = np_points[db_labels!=-1]
    # print len(new_db_labels)
    # print len(new_location)
    # print new_information

    label_unique = np.unique(new_db_labels)
    cluster_center = np.zeros((len(label_unique),2))
    for label in label_unique:
        sub_location = new_location[new_db_labels==label]
        temp_center = np.mean(sub_location,axis=0)
        cluster_center[int(label)] = temp_center
    # print cluster_center
    return cluster_center

#
# print(mode_cluster(6))

def mode_start_end_coverage(section,cluster,eps):
    mode_change_pnts=[]
    # print(tran_mat)
    num_sec=0
    centers=cluster
    # print(centers)
    try:
        if Include_place_2(centers,section['section_start_point']['coordinates'],eps) and \
                    Include_place_2(centers,section['section_end_point']['coordinates'],eps):
            return 1
        else:
            return 0
    except:
            return 0
# print(mode_start_end_coverage(5,105,2))
# print(mode_start_end_coverage(6,600,2))

# This is currently only used in this file, so it is fine to use only really
# user confirmed modes. We don't want to learn on trips where we don't have
# ground truth.
def get_mode_share_by_count(lst):
    # input here is a list of sections
    displayModeList = getDisplayModes()
    # logging.debug(displayModeList)
    modeCountMap = {}
    for mode in displayModeList:
        modeCountMap[mode['mode_name']] = 0
        for section in lst:
            if section['confirmed_mode']==mode['mode_id']:
                modeCountMap[mode['mode_name']] +=1
            elif section['mode']==mode['mode_id']:
                modeCountMap[mode['mode_name']] +=1
    return modeCountMap

# This is currently only used in this file, so it is fine to use only really
# user confirmed modes. We don't want to learn on trips where we don't have
# ground truth.
def get_mode_share_by_count(list_idx):
    Sections=get_section_db()
    ## takes a list of idx's
    AllModeList = getAllModes()

    MODE = {}
    MODE2= {}
    for mode in AllModeList:
        MODE[mode['mode_id']]=0
    for _id in list_idx:
        section=Sections.find_one({'_id': _id})
        mode_id = section['confirmed_mode']
        try:
            MODE[mode_id] += 1
        except KeyError:
            MODE[mode_id] = 1
    # print(sum(MODE.values()))
    if sum(MODE.values())==0:
        for mode in AllModeList:
            MODE2[mode['mode_id']]=0
        # print(MODE2)
    else:
        for mode in AllModeList:
            MODE2[mode['mode_id']]=old_div(MODE[mode['mode_id']],sum(MODE.values()))
    return MODE2

def cluster_route_match_score(section,step1=100000,step2=100000,method='lcs',radius1=2000,threshold=0.5):
    userRouteClusters=get_routeCluster_db().find_one({'$and':[{'user':section['user_id']},{'method':method}]})['clusters']
    route_seg = getRoute(section['_id'])

    dis=999999
    medoid_ids=list(userRouteClusters.keys())
    if len(medoid_ids)!=0:
        choice=medoid_ids[0]
        for idx in list(userRouteClusters.keys()):
            route_idx=getRoute(idx)
            try:
                dis_new=fullMatchDistance(route_seg,route_idx,step1,step2,method,radius1)
            except RuntimeError:

                dis_new=999999
            if dis_new<dis:
                dis=dis_new
                choice=idx
    # print(dis)
    # print(userRouteClusters[choice])
    if dis<=threshold:
        cluster=userRouteClusters[choice]
        cluster.append(choice)
        ModePerc=get_mode_share_by_count(cluster)
    else:
        ModePerc=get_mode_share_by_count([])

    return ModePerc

def transit_route_match_score(section,step1=100000,step2=100000,method='lcs',radius1=2500,threshold=0.5):
    Transits=get_transit_db()
    transitMatch={}
    route_seg=getRoute(section['_id'])
    for type in Transits.distinct('type'):
        for entry in Transits.find({'type':type}):
            transitMatch[type]=matchTransitRoutes(route_seg,entry['stops'],step1,step2,method,radius1,threshold)
            if transitMatch[entry['type']]==1:
                break
    return transitMatch

def transit_stop_match_score(section,radius1=300):
    Transits=get_transit_db()
    transitMatch={}
    route_seg=getRoute(section['_id'])
    for type in Transits.distinct('type'):
        for entry in Transits.find({'type':type}):
            transitMatch[type]=matchTransitStops(route_seg,entry['stops'],radius1)
            if transitMatch[entry['type']]==1:
                break
    return transitMatch

def select_inferred_mode(prediction_list):
    # We currently only support a single prediction
    assert(len(prediction_list) == 1)
    curr_prediction = prediction_list[0]
    assert curr_prediction.algorithm_id == ecwm.AlgorithmTypes.SEED_RANDOM_FOREST or \
            curr_prediction.algorithm_id == ecwm.AlgorithmTypes.SIMPLE_RULE_ENGINE
   
    prediction_map = curr_prediction["predicted_mode_map"]
    max_value = max(prediction_map.values())
    logging.debug("max confidence in prediction map = %s" % max_value)
    keys_for_max_value = [k for (k, v) in prediction_map.items() if v == max_value]
    logging.debug("max keys in prediction map = %s" % keys_for_max_value)
    if len(keys_for_max_value) == 1:
        return keys_for_max_value[0]
    else:
        classes_for_max_value = [ecwm.PredictedModeTypes[key].value for key in keys_for_max_value]
        logging.debug("classes for max_value = %s" % classes_for_max_value)
        min_class = min(classes_for_max_value)
        logging.debug("min_class = %s" % min_class)
        return ecwm.PredictedModeTypes(min_class).name
