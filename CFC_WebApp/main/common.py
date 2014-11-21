from __future__ import division
from random import randrange
import logging
from tripManager import calDistance, travel_time
from pymongo import MongoClient
from datetime import datetime, timedelta
from dateutil import parser
from pytz import timezone
from get_database import get_mode_db, get_section_db, get_trip_db, get_test_db
from userclient import getClientSpecificQueryFilter
from dao.client import Client

# from pylab import *
# from scipy.interpolate import Rbf
# import simplekml
# import matplotlib.cm as cm
import copy
skippedModeList = ["transport", "underground", "not a trip"]

def getAllModes():
  Modes = get_mode_db()
  # Modes = MongoClient().Test_database.Test_Modes
  modes = []
  for mode in Modes.find():
    modes.append(mode)
  return modes

def getDisplayModes():
  allModes = getAllModes()
  displayModes = []
  for mode in allModes:
    if (mode['mode_name'] not in skippedModeList):
      displayModes.append(mode)
  return displayModes

def getDistinctUserCount(query):
  Sections = get_section_db()

  distinctUserCount = len(Sections.find(query).distinct("user_id"))
  logging.debug("Found %s distinct users " % distinctUserCount)
  return distinctUserCount

def convertToAvg(totalMap, nUsers):
  avgMap = {}
  for (key, value) in totalMap.items():
    avgMap[key] = float(value)/nUsers
  return avgMap

# TODO: We need to figure out whether to pass in mode or modeID
def getQuerySpec(user, modeId,start,end):
  Sections = get_section_db()
  # Sections=MongoClient().Test_database.Test_Sections
  queryComponents = []
  if modeId is not None:
    queryComponents.append(getConfirmationModeQuery(modeId))
  else:
    queryComponents.append(getConfirmationModeQuery({"$ne": ""}))
  queryComponents.append({"type": "move"})
  queryComponents.append({"section_start_datetime": {"$gte": start, "$lt": end}})
  if user != None:
    queryComponents.append({"user_id": user})
  query = {"$and" : queryComponents }
  return query

def getTripCountForMode(user, modeId,start,end):
  return get_section_db().find(getQuerySpec(user, modeId,start,end)).count()

def getModeShare(user,start,end):
  displayModeList = getDisplayModes()
  # logging.debug(displayModeList)

  modeCountMap = {}
  for mode in displayModeList:
    tripCount = getTripCountForMode(user, mode["mode_id"],start,end)
    modeCountMap[mode['mode_name']] = tripCount
  return modeCountMap

def getDistance(sectionList):
    distance_list = []

    for section in sectionList:
        points = section['track_points']
        if len(points) >= 2:
            point_curr = points[0]['track_location']['coordinates']
            distance = 0
            for i in range(len(points)-1):
                point_next = points[i+1]['track_location']['coordinates']
                distance = distance + calDistance(point_curr, point_next)
                point_curr = point_next
            distance_list.append(distance)
        else:
            distance_list.append(0)
    return distance_list

def getModeShareDistance(user,start,end):
  displayModeList = getDisplayModes()
  modeDistanceMap = {}
  for mode in displayModeList:
    spec = getQuerySpec(user, mode['mode_id'],start,end)
    distanceForMode = getDistanceForMode(spec)
    modeDistanceMap[mode['mode_name']] = distanceForMode
  return modeDistanceMap


def getDistanceForMode(spec):
  distanceList = []
  totalDist = 0
  projection = {'distance': True, '_id': False}
  for section in get_section_db().find(spec, projection):
#  for section in get_section_db().find(spec):
#    logging.debug("found section %s" % section)
#    logging.debug("found section with %s %s %s %s %s %s %s" %
#       (section['trip_id'], section['section_id'], section['confirmed_mode'],
#           section['user_id'], section['type'], section.get('auto_confirmed'), section.get('distance')))
    if section['distance']!=None:
        totalDist = totalDist + section['distance']
    else:
        logging.warning("distance key not found in projection, returning zero...")
      # returning zero for the distance
        pass
  # logging.debug("for sectionList %s, distanceList = %s" % (len(sectionList), distanceList))
  distanceForMode = totalDist
  return distanceForMode

def generateRandomResult(category_list):
    result = {}
    maxPct = int(100.0/len(category_list))
    pctSum = 0
    for category in category_list[:-1]:
        currPct = randrange(maxPct)
        pctSum = pctSum + currPct
        result[category] = currPct

    result[category_list[-1]] = 100 - pctSum
    return result

def most_common(lst,radius):
    # list of tracking points
    max_pnts=0
    most_common_pnt=lst[0]
    for pnt in lst:
        count=0
        for pnt2 in lst:
            if calDistance(pnt['track_location']['coordinates'],pnt2['track_location']['coordinates'])<=radius:
                count=count+1
        if max_pnts<=count:
            max_pnts=count
            most_common_pnt=pnt
    return most_common_pnt['track_location']['coordinates']

def most_common_2(lst,radius):
    # list of tracking points
    max_pnts=0
    most_common_pnt=lst[0]
    for pnt in lst:
        count=0
        for pnt2 in lst:
            if calDistance(pnt,pnt2)<=radius:
                count=count+1
        if max_pnts<=count:
            max_pnts=count
            most_common_pnt=pnt
    return most_common_pnt

def get_date(lst_pnt):
    # a list of tracking points
    list_date=[]
    # print(len(lst_pnt))
    for pnt in lst_pnt:
        time1=parse_time(pnt['time'])
        list_date.append(time1.replace(hour=0, minute=0, second=0, microsecond=0))
    # print(len(list_date))
    # print(list_date)
    list_date_1=set(list_date)
    # print(list_date)
    list_date_2=list(list_date_1)
    # print(list_date)
    return list_date_2

def Is_date(time,day):
    # a tracking point
    time1=parse_time(time)
    if time1.isoweekday()==day:
        return True
    else:
        return False

def Is_weekday(time):
    time1=parse_time(time)
    if time1.isoweekday() in range (1,6):
        return True
    else:
        return False

def Is_workhour(time):
    # a tracking point
    time1=parse_time(time)
    if time1.hour in range (8,18):
        return True
    else:
        return False

def Is_sameday(time11,time22):
    time1=parse_time(time11)
    time2=parse_time(time22)
    if time1.date()==time2.date():
        return True
    else:
        return False

def get_first_daily_point(lst):
    # list of tracking points
    earlist_start=5
    list_date=[]
    list_home=[]
    list_date=get_date(lst)
    # print(list_date)
    for date in list_date:
        # print(date)
        day_1=[]
        for pnt in lst:
            time1=parser.parse(pnt['time'])
            if (time1 - date).total_seconds()/3600>earlist_start and (time1 - date).total_seconds()/3600<24:
                day_1.append(pnt)
        if len(day_1)!=0:
            day_1 = sorted(day_1, key=lambda k: parser.parse(k['time']))
            list_home.append(day_1[0])
    return list_home

def get_last_daily_point(lst):
    # list of tracking points
    earlist_start=5
    list_date=[]
    list_work=[]
    # print('lst in get last pnt is %s' % len(lst))
    # print(lst)
    list_date_old=get_date(lst)
    list_date.extend(list_date_old)
    # print(list_date)
    # print(type(list_date))
    for date in list_date_old:
        # print(date)
        # print(date - timedelta(days=1))
        # print(date - timedelta(days=1) not in list_date)
        if (date - timedelta(days=1)) not in list_date:
            # print(type(date - timedelta(days=1)))
            list_date.append(date - timedelta(days=1))
    # print(list_date)
    # print(list_date)
    for date in list_date:
        day_1=[]
        day_2=[]
        for pnt in lst:
            time1=parser.parse(pnt['time'])
            if (time1 - date).total_seconds()/3600>earlist_start and (time1 - date).total_seconds()/3600<=24:
                day_1.append(pnt)
            if (time1 - date).total_seconds()/3600>24 and (time1 - date).total_seconds()/3600<24+earlist_start:
                day_2.append(pnt)
        if len(day_2)!=0:
            day_2 = sorted(day_2, key=lambda k: parser.parse(k['time']))
            list_work.append(day_2[-1])
        elif len(day_2)==0 and len(day_1)!=0:
            day_1 = sorted(day_1, key=lambda k: parser.parse(k['time']))
            list_work.append(day_1[-1])
        else:
            continue
    return list_work

def get_static_pnts(lst):
    # parameter
    interval= 600
    new_lst=[]
    new_lst.extend(lst)
    for i in range(len(lst)-1):
        loc1=lst[i]['track_location']['coordinates']
        loc2=lst[i+1]['track_location']['coordinates']
        if Is_sameday(lst[i]['time'],lst[i+1]['time'])==True and calDistance(loc1,loc2)<=100:
            duration=travel_time(lst[i]['time'],lst[i+1]['time'])
            for j in range(int(duration/interval/2)):
                new_lst.append(lst[i])
                new_lst.append(lst[i+1])
    new_lst=sorted(new_lst, key=lambda k: parser.parse(k['time']))
    return new_lst


def Is_place(place1,place,radius):
    # print(place)
    if calDistance(place1['coordinates'],place)<radius:
        return True
    else:
        return False

def Is_place_2(place1,place,radius):
    # print(place)
    if calDistance(place1,place)<radius:
        return True
    else:
        return False

def Include_place(lst,place,radius):
    # list of tracking points
    count=0
    for pnt in lst:
        count=count+(1 if calDistance(pnt['coordinates'],place)<=radius else 0)
    if count>0:
        return True
    else:
        return False

def Include_place_2(lst,place,radius):
    # list of tracking points
    count=0
    for pnt in lst:
        count=count+(1 if calDistance(pnt,place)<=radius else 0)
    if count>0:
        return True
    else:
        return False

def calculate_appearance_rate(lst,place):
    # list of tracking points
    num_app=0
    for pnt in lst:
        if Is_place(pnt['track_location'],place,100)==True:
            num_app=num_app+1
    return num_app/len(lst)

def get_mode_share_by_distance(spec):
    # input here is a list of sections
    displayModeList = getDisplayModes()
    # print(displayModeList)
    # logging.debug(displayModeList)
    modeDistanceMap = {}
    for mode in displayModeList:
        modeDistanceMap[mode['mode_name']] = 0
        modeSpec = addModeIdToSpec(spec, mode['mode_id'])
        distanceForMode = getDistanceForMode(modeSpec)
        logging.debug("distanceForMode %s = %s" % (mode, distanceForMode))
        modeDistanceMap[mode['mode_name']] = distanceForMode
    return modeDistanceMap


def addModeIdToSpec(spec, modeId):
  modeSpec = getConfirmationModeQuery(modeId)
  logging.debug("Adding modeId %s to spec %s" % (modeId, spec))
  return addFilterToSpec(spec, modeSpec)

def addFilterToSpec(spec, addlFilterSpec):
  retSpec = copy.deepcopy(spec)
  if spec == None:
    retSpec = addlFilterSpec
  else:
    if "$and" in spec:
        retSpec['$and'].append(addlFilterSpec)
    else:
        retSpec = {"$and": [spec, addlFilterSpec]}
  # logging.debug("after adding addlSpec %s to %s, spec is %s" % (addlFilterSpec, spec, retSpec))
  return retSpec

def parse_time(time):
    if time[-1]=='Z':
        time2=parser.parse(time)
        time2 = time2.astimezone(timezone('US/Pacific'))
    else:
        time2=parser.parse(time)# input here is a list of sections

    return time2

def berkeley_area():
    # SW=[-122.265701,37.867951]
    # SE=[-122.247870,37.870932]
    # NE=[-122.253535,37.876962]
    # NW=[-122.266324,37.875149]
    SW=[37.867951,-122.265701]
    SE=[37.870932,-122.247870]
    NE=[37.876962,-122.253535]
    NW=[37.875149,-122.266324]

    return [SW,SE,NE,NW]

def Inside_polygon(pnt,poly):

    n = len(poly)
    inside =False

    p1x,p1y = poly[0]
    for i in range(n+1):
        p2x,p2y = poly[i % n]
        if pnt['coordinates'][1] > min(p1y,p2y):
            if pnt['coordinates'][1] <= max(p1y,p2y):
                if pnt['coordinates'][0] <= max(p1x,p2x):
                    if p1y != p2y:
                        xinters = (pnt['coordinates'][1]-p1y)*(p2x-p1x)/(p2y-p1y)+p1x
                    if p1x == p2x or pnt['coordinates'][0] <= xinters:
                        inside = not inside
        p1x,p1y = p2x,p2y

    return inside

# Consider passing in a time range as well. We could just do it right now,
# but that might be over engineering
def getClassifiedRatio(uuid, start, end):
    defaultQueryList = [ {'source':'Shankari'},
                         {'user_id':uuid},
                         {'predicted_mode': { '$exists' : True } },
                         { 'type': 'move' },
                         { 'section_start_datetime': {'$gt': start, '$lt': end}}]
    clientSpecificQuery = getClientSpecificQueryFilter(uuid)
    completeQueryList = defaultQueryList + clientSpecificQuery
    logging.debug("completeQueryList = %s" % completeQueryList)
    unclassifiedQueryList = completeQueryList + [{'confirmed_mode': ''}]
    classifiedQueryList = completeQueryList + [{'confirmed_mode': {"$ne": ''}}]

    unclassifiedCount = get_section_db().find({'$and': unclassifiedQueryList}).count()
    classifiedCount = get_section_db().find({'$and': classifiedQueryList}).count()
    totalCount = get_section_db().find({'$and': completeQueryList}).count()
    logging.info("unclassifiedCount = %s, classifiedCount = %s, totalCount = %s" % (unclassifiedCount, classifiedCount, totalCount))
    assert(unclassifiedCount + classifiedCount == totalCount)
    if totalCount > 0:
        return float(classifiedCount)/totalCount
    else:
        return 0

# def generategrid(latsouth, latnorth, loneast, lonwest,ncell):
#     xgrid = np.linspace(lonwest, loneast, ncell)
#     ygrid = np.linspace(latnorth, latsouth, ncell)
#     mX, mY = np.meshgrid(xgrid, ygrid)
#     ngridX = mX.reshape(ncell*ncell, 1);
#     ngridY = mY.reshape(ncell*ncell, 1);
#     return np.concatenate((ngridX, ngridY), axis=1)
# 
# def heatmap(cod,value,ncell,eps,filename, show=False):
# 
#     lat=cod[:,0]
#     lon=cod[:,1]
#     # print(lat)
#     # print(lon)
#     # print(value)
#     rbf=Rbf(lat,lon,value,epsilon=eps)
#     latsouth=np.min(lat)-0.2
#     latnorth=np.max(lat)+0.2
#     loneast=np.max(lon)+0.2
#     lonwest=np.min(lon)-0.2
#     # print(latsouth,latnorth,loneast,lonwest)
#     X=generategrid(latsouth, latnorth, loneast, lonwest,ncell)[:,0]
#     Y=generategrid(latsouth, latnorth, loneast, lonwest,ncell)[:,1]
#     Z=rbf(Y,X)
#     # Znew=np.zeros(Z.shape[0])
#     # for i in range(Z.shape[0]):
#     #     Znew[i]=Z[i,0]
#     # print(X.reshape(ncell,ncell))
#     # print(Y.reshape(ncell,ncell))
#     fig = plt.figure(frameon=False)
#     plt.imshow(Z.reshape(ncell,ncell),cmap=cm.RdYlGn,interpolation='bilinear', origin='lower',alpha=0.6,
#                extent=(lonwest, loneast, latsouth, latnorth), aspect='auto')
#     plt.axis('off')
#     plt.scatter(lon, lat, marker='o',linewidths=0, s=150, c=value, cmap=cm.RdYlGn)
#     # plt.xlim(lonwest, loneast)
#     # plt.ylim(latsouth, latnorth)
#     fig.savefig(filename, transparent=True, bbox_inches='tight')
#     if show:
#         plt.show()
#     # kml = simplekml.Kml()
#     ground = kml.newgroundoverlay(name=filename)
#     ground.icon.href = filename+'.png'
#     ground.latlonbox.north = latnorth
#     ground.latlonbox.south = latsouth
#     ground.latlonbox.east = loneast
#     ground.latlonbox.west = lonwest
#     # kml.save(filename+'.kml')

def getConfirmationModeQuery(mode):
  return {'$or': [{'corrected_mode': mode},
                  {'$and': [{'corrected_mode': {'$exists': False}}, {'confirmed_mode': mode}]}, 
                  {'$and': [{'corrected_mode': {'$exists': False}},
                            {'confirmed_mode': {'$exists': False}}] + Client.getClientConfirmedModeQueries(mode)}]}

def convertModeNameToIndex(ModeDb, modeName):
  logging.debug("modedb size = %s" % ModeDb.find().count())
  return int(''.join(map(str, [mode['mode_id'] for mode in ModeDb.find({'mode_name':modeName})]))) \
    if ModeDb.find({'mode_name':modeName}).count()!=0 else modeName
