
from __future__ import division
import math
import logging
import numpy as np
from pymongo import MongoClient
import utm
from sklearn.cluster import DBSCAN

Sections = MongoClient('localhost').Stage_database.Stage_Sections
Modes=MongoClient('localhost').Stage_database.Stage_Modes
# Returns distance in m
def Include_place(lst,place,radius):
    # list of tracking points
    count=0
    for pnt in lst:
        count=count+(1 if calDistance(pnt,place)<=radius else 0)
    if count>0:
        return True
    else:
        return False

def calDistance(point1, point2):
    earthRadius = 6371000
    # Point is in GeoJSON format, ie (lng, lat)
    dLat = math.radians(point1[1]-point2[1])
    dLon = math.radians(point1[0]-point2[0])
    lat1 = math.radians(point1[1])
    lat2 = math.radians(point2[1])

    a = (math.sin(dLat/2) ** 2) + ((math.sin(dLon/2) ** 2) * math.cos(lat1) * math.cos(lat2))
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
    d = earthRadius * c

    return d

# The speed is in m/s
def calSpeed(trackpoint1, trackpoint2):
  from dateutil import parser
  distanceDelta = calDistance(trackpoint1['track_location']['coordinates'],
                              trackpoint2['track_location']['coordinates'])
  timeDelta = parser.parse(trackpoint2['time']) - parser.parse(trackpoint1['time'])
  logging.debug("while calculating speed form %s -> %s, distanceDelta = %s, timeDelta = %s" %
                (trackpoint1, trackpoint2, distanceDelta, timeDelta))
  if timeDelta.total_seconds() != 0:
    return distanceDelta / timeDelta.total_seconds()
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

def calHCR(segment):
    trackpoints = segment['track_points']
    if len(trackpoints) < 3:
        return 0
    else:
        HCNum = 0
        for (i, point) in enumerate(trackpoints[:-2]):
            currPoint = point
            nextPoint = trackpoints[i+1]
            nexNextPt = trackpoints[i+2]
            HC = calHC(currPoint['track_location']['coordinates'], nextPoint['track_location']['coordinates'], \
                       nexNextPt['track_location']['coordinates'])
            if HC >= 15:
                HCNum += 1
        segmentDist = segment['distance']
        if segmentDist!= None and segmentDist != 0:
            HCR = HCNum/segmentDist
            return HCR
        else:
            return 0


def calSR(segment):
    trackpoints = segment['track_points']
    if len(trackpoints) < 2:
        return 0
    else:
        stopNum = 0
        for (i, point) in enumerate(trackpoints[:-1]):
            currPoint = point
            nextPoint = trackpoints[i+1]

            currVelocity = calSpeed(currPoint, nextPoint)
            if currVelocity != None and currVelocity <= 0.75:
                stopNum += 1

        segmentDist = segment['distance']
        if segmentDist != None and segmentDist != 0:
            return stopNum/segmentDist
        else:
            return 0

def calVCR(segment):
    trackpoints = segment['track_points']
    if len(trackpoints) < 3:
        return 0
    else:
        Pv = 0
        for (i, point) in enumerate(trackpoints[:-2]):
            currPoint = point
            nextPoint = trackpoints[i+1]
            nexNextPt = trackpoints[i+2]
            velocity1 = calSpeed(currPoint, nextPoint)
            velocity2 = calSpeed(nextPoint, nexNextPt)
            if velocity1 != None and velocity2 != None:
                if velocity1 != 0:
                    VC = abs(velocity2 - velocity1)/velocity1
                else:
                    VC = 0
            else:
                VC = 0

            if VC > 0.7:
                Pv += 1

        segmentDist = segment['distance']
        if segmentDist != None and segmentDist != 0:
            return Pv/segmentDist
        else:
            return 0

def calSegmentDistance(segment):
  return segment['distance']

def calSpeeds(segment):
  trackpoints = segment['track_points']
  if len(trackpoints) == 0:
    return None

  speeds = np.zeros(len(trackpoints) - 1)
  for (i, point) in enumerate(trackpoints[:-1]):
    currPoint = point
    nextPoint = trackpoints[i+1]
    currSpeed = calSpeed(currPoint, nextPoint)
    if currSpeed != None:
      speeds[i] = currSpeed
    logging.debug("Returning vector of length %s while calculating speeds for trackpoints of length %s " % (speeds.shape, len(trackpoints)))
  return speeds

def calAvgSpeed(segment):
  timeDelta = segment['section_end_datetime'] - segment['section_start_datetime']
  if timeDelta.total_seconds() != 0:
    return segment['distance'] / timeDelta.total_seconds()
  else:
    return None

# In order to calculate the acceleration, we do the following.
# point0: (loc0, t0), point1: (loc1, t1), point2: (loc2, t2), point3: (loc3, t3)
# becomes
# speed0: ((loc1 - loc0) / (t1 - t0)), speed1: ((loc2 - loc1) / (t2-t1)),
# speed2: ((loc3 - loc2) / (t3 - t2)
# becomes
# segment0: speed0 / (t1 - t0), segment1: (speed1 - speed0)/(t2-t1),
# segment2: (speed2 - speed1) / (t3-t2)

def calAccels(segment):
  from dateutil import parser

  speeds = calSpeeds(segment)
  trackpoints = segment['track_points']

  if speeds == None:
    return None

  accel = np.zeros(len(speeds) - 1)
  prevSpeed = 0
  for (i, speed) in enumerate(speeds[0:-1]):
    currSpeed = speed # speed0
    speedDelta = currSpeed - prevSpeed # (speed0 - 0)
    # t1 - t0
    timeDelta = parser.parse(trackpoints[i+1]['time']) - parser.parse(trackpoints[i]['time'])
    logging.debug("while calculating accels from %s -> %s, speedDelta = %s, timeDelta = %s" %
      (trackpoints[i+1], trackpoints[i], speedDelta, timeDelta))
    if timeDelta.total_seconds() != 0:
      accel[i] = speedDelta/(timeDelta.total_seconds())
      # logging.debug("resulting acceleration is %s" % accel[i])
    prevSpeed = currSpeed
  return accel

def getIthMaxSpeed(segment, i):
  # python does not appear to have a built-in mechanism for returning the top
  # ith max. We would need to write our own, possibly by sorting. Since it is
  # not clear whether we ever actually need this (the paper does not explain
  # which i they used), we just return the max.
  assert(i == 1)
  speeds = calSpeeds(segment)
  return np.amax(speeds)

def getIthMaxAccel(segment, i):
  # python does not appear to have a built-in mechanism for returning the top
  # ith max. We would need to write our own, possibly by sorting. Since it is
  # not clear whether we ever actually need this (the paper does not explain
  # which i they used), 
  assert(i == 1)
  accels = calAccels(segment)
  return np.amax(accels)

def calSpeedDistParams(speeds):
  return (np.mean(speeds), np.std(speeds))

# def user_tran_mat(user):
#     user_sections=[]
#     # print(tran_mat)
#     query = {"$and": [{'type': 'move'},{'user_id':user},\
#                       {'$or': [{'confirmed_mode':1}, {'confirmed_mode':3},\
#                                {'confirmed_mode':5},{'confirmed_mode':6},{'confirmed_mode':7}]}]}
#     # print(Sections.find(query).count())
#     for section in Sections.find(query).sort("section_start_datetime",1):
#         user_sections.append(section)
#     if Sections.find(query).count()>=2:
#         tran_mat=np.zeros([Modes.find().count(), Modes.find().count()])
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
#     tran_mat=np.zeros([Modes.find().count(), Modes.find().count()])
#     for user in Sections.distinct("user_id"):
#         user_sections=[]
#         # print(tran_mat)
#         query = {"$and": [{'type': 'move'},{'user_id':user},\
#                           {'$or': [{'confirmed_mode':1}, {'confirmed_mode':3},\
#                                    {'confirmed_mode':5},{'confirmed_mode':6},{'confirmed_mode':7}]}]}
#         # print(Sections.find(query).count())
#         for section in Sections.find(query).sort("section_start_datetime",1):
#             user_sections.append(section)
#         if Sections.find(query).count()>=2:
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
    # print(tran_mat)
    query = {"$and": [{'type': 'move'},\
                      {'confirmed_mode':mode}]}
    # print(Sections.find(query).count())
    logging.debug("Trying to find cluster locations for %s trips" % (Sections.find(query).count()))
    for section in Sections.find(query).sort("section_start_datetime",1):
        try:
            mode_change_pnts.append(section['section_start_point']['coordinates'])
            mode_change_pnts.append(section['section_end_point']['coordinates'])
        except:
            pass
    # print(user_change_pnts)
    # print(len(mode_change_pnts))
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

def mode_start_end_coverage(segment,cluster,eps):
    mode_change_pnts=[]
    # print(tran_mat)
    num_sec=0
    centers=cluster
    # print(centers)
    try:
        if Include_place(centers,segment['section_start_point']['coordinates'],eps) and \
                    Include_place(centers,segment['section_end_point']['coordinates'],eps):
            return 1
        else:
            return 0
    except:
            return 0
# print(mode_start_end_coverage(5,105,2))
# print(mode_start_end_coverage(6,600,2))
