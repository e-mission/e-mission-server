from __future__ import print_function
from __future__ import division
from __future__ import unicode_literals
from __future__ import absolute_import
from future import standard_library
standard_library.install_aliases()
from builtins import range
from builtins import *
from past.utils import old_div
import urllib.request, urllib.error, urllib.parse
import csv
import math
import numpy
import datetime, pytz
from os import remove
from pymongo import MongoClient
import sys

print("old pythonpath = %s" % sys.path)
sys.path.extend(['', '/home/ubuntu/anaconda/lib/python27.zip',
  '/home/ubuntu/anaconda/lib/python2.7',
  '/home/ubuntu/anaconda/lib/python2.7/plat-linux2',
  '/home/ubuntu/anaconda/lib/python2.7/lib-tk',
  '/home/ubuntu/anaconda/lib/python2.7/lib-old',
  '/home/ubuntu/anaconda/lib/python2.7/lib-dynload',
  '/home/ubuntu/anaconda/lib/python2.7/site-packages',
  '/home/ubuntu/anaconda/lib/python2.7/site-packages/PIL',
  '/home/ubuntu/anaconda/lib/python2.7/site-packages/setuptools-2.2-py2.7.egg'])
print("new pythonpath = %s" % sys.path)

import numpy

# Procedure that takes as input strings deonting the tester name, test phone number, date and time, 
# and a temporary file path name. Output is a list of lists containing GPS data collected over 
# the last 24 hours for that tester and phone.

def getNewGPSData(testerName, phoneNum, lastUpdate, gpsFilePath):

    url = 'http://' + phoneNum + 'gp.appspot.com/gaeandroid?query=1'
    data = urllib.request.urlopen(url)
    
    localFile = open(gpsFilePath, 'w')
    localFile.write(data.read())
    localFile.close()

    year = int(lastUpdate[0:4])
    month = int(lastUpdate[4:6])
    day = int(lastUpdate[6:8])
    hours = int(lastUpdate[9:11])
    minutes = int(lastUpdate[11:13])
    seconds = int(lastUpdate[13:15])
    endTime = 1000 * int(datetime.datetime(year, month, day, hours, minutes, seconds).strftime('%s'))
    startTime = endTime - (24 * 60 * 60 * 1000) 

    gpsData = []
    with open(gpsFilePath, 'rU') as csvfile:
        for row in csv.reader(csvfile, delimiter = '\t'):
            try:
                if int(row[1]) >= startTime and int(row[1]) <= endTime:
                    tList = []
                    for element in row:
                        try:
                            tList.append(float(element))    
                        except:
                            tList.append(element)    
                    gpsData.append(tList)
            except:
                pass            
    gpsData = sorted(gpsData, key = lambda x: int(x[1]))
    remove(gpsFilePath)
    return gpsData


# Function that uses the haversine formula to calculate the 'great-circle' distance in meters
# between two points whose latitutde and longitude are known

def calDistance(point1, point2):

    earthRadius = 6371000 
    dLat = math.radians(point1[0]-point2[0])
    dLon = math.radians(point1[1]-point2[1])    
    lat1 = math.radians(point1[0])
    lat2 = math.radians(point2[0])
    
    a = (math.sin(old_div(dLat,2)) ** 2) + ((math.sin(old_div(dLon,2)) ** 2) * math.cos(lat1) * math.cos(lat2))
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
    d = earthRadius * c 
    
    return d


# Function that takes as input a point and a list of points, where a point is itself a list containing 
# the elements in the row in the input file corresponding to that point. The function outputs the maximum 
# distance, in meters, from the 95% CI around that point to the 95% CI around any point in the list of points

def calDistanceToPoint(point, points):
    maxDistance = 0
    for i in range(0, len(points)):
        dist = calDistance(point[2:4], points[i][2:4]) - point[4] - points[i][4]
        if dist > maxDistance:
            maxDistance = dist
    return maxDistance
    

# Function that takes as input two lists of points, where a point is itself a list containing 
# the elements in the row in the input file corresponding to that point. The function outputs the 
# distance, in meters, between the median points in the two lists

def calDistanceBetweenPoints(points1, points2):
    latLon1, latLon2 = numpy.zeros(shape = (len(points1), 2)), numpy.zeros(shape = (len(points2), 2))
    for i in range(0, len(points1)):
        latLon1[i, 0] = points1[i][2]
        latLon1[i, 1] = points1[i][3]
    for i in range(0, len(points2)):
        latLon2[i, 0] = points2[i][2]
        latLon2[i, 1] = points2[i][3]
    point1 = [numpy.median(latLon1[:, 0]), numpy.median(latLon1[:, 1])]
    point2 = [numpy.median(latLon2[:, 0]), numpy.median(latLon2[:, 1])]    
    return calDistance(point1, point2)


# Procedure that takes as input the start and end points to an event, the list of events and holes,
# the list comprising the raw GPS data and the threshold for labelling a gap in the data a hole,
# and infers holes in the data and splits the event accordingly into multiple events

def inferHoles(eventStart, eventEnd, events, holes, gpsTraces, minSamplingRate):
    j = eventStart + 1
    while j <= eventEnd:
        while (j < eventEnd and 
                gpsTraces[j][1] - gpsTraces[j - 1][1] < minSamplingRate):
            j += 1
        if gpsTraces[j][1] - gpsTraces[j - 1][1] >= minSamplingRate:
            holes.append([j - 1, j])
            if j - 1 > eventStart:
                events.append([eventStart, j - 1])
        else:
            events.append([eventStart, j])
        eventStart, j = j, j + 1
    
    
# Method that takes as input the list containing GPS data, called gpsTraces, and two empty lists, 
# called trips and activities. 
#
# Each element of trips is a tuple and corresponds to a particular trip. The elements of the tuple are the 
# indices of the corresponding GPS data points in gpsTraces for where the trip began and ended, respectively.
# Similarly, each element of activities is a tuple and corresponds to a particular activity. The elements 
# of the tuple are the indices of the corresponding GPS data point in gpsTraces for where the activity began 
# and ended, respectively.
# 
# An activity is defined as a set of GPS points over a minimum duration of minDuration milliseconds that fall within 
# a circle of radius maxRadius meters. The minimum interval between successive activites must be at least 
# minInterval milliseconds, for them to be recorded as separate activities.
#
# GPS traces whose accuracy is above gpsAccuracyThreshold meters are ignored.

def inferTripActivity(gpsTraces, minDuration, maxRadius, minSeparationDistance, 
        minSeparationTime, minSamplingRate, gpsAccuracyThreshold):
    
    trips, activities, holes = [], [], []
    
    # Infer activities
    i = 0
    while i < len(gpsTraces) - 1:
               
        # Skip over any black points at the beginning 
        while i < len(gpsTraces) - 1 and gpsTraces[i][4] >= gpsAccuracyThreshold:
            i += 1

        # Create a collection of successive points that lie within a circle of radius maxRadius meters, such that no
        # two consecutive points in space are separated by more than minSamplingRate milliseconds
        j = i + 1
        
        points = [gpsTraces[i]]
        while (j < len(gpsTraces) and gpsTraces[j][4] < gpsAccuracyThreshold 
                and gpsTraces[j][1] - gpsTraces[j-1][1] < minSamplingRate
                and calDistanceToPoint(gpsTraces[j], points) < maxRadius):
            points.append(gpsTraces[j])
            j += 1

        # Check for black points
        k = j 
        while k < len(gpsTraces) and gpsTraces[k][4] >= gpsAccuracyThreshold:
            k += 1
        if k > j:
            if k < len(gpsTraces):
                if calDistanceToPoint(gpsTraces[k], points) < maxRadius:
                    j = k + 1

        # Check if the duration over which these points were collected exceeds minDuration milliseconds
        if gpsTraces[j-1][1] - gpsTraces[i][1] > minDuration:
            
            # Check if the activity is separated in space from previous activity by at least minSeparationDistance meters
            # and separated in time by minSeparationTime milliseconds
            if (len(activities) > 0 and gpsTraces[j-1][1] - gpsTraces[activities[-1][1]][1] < minSeparationTime
                    and calDistanceBetweenPoints(gpsTraces[activities[-1][0]:activities[-1][1]], 
                    gpsTraces[i:j-1]) < minSeparationDistance):                
                activities[-1][-1] = j-1
            else:
                activities.append([i, j-1])
            i = j - 1
        else:
            i += 1
        
        if k == len(gpsTraces):
            break

    # Impute trips and identify holes in data
    numActivities, newActivities = len(activities), []
    if numActivities != 0:
        
        # Check if the GPS log begins with a trip
        if activities[0][0] != 0:
            inferHoles(0, activities[0][0], trips, holes, gpsTraces, minSamplingRate)
        
        # Interpolate trips from activities and identify holes in activities
        if numActivities > 1:
            for i in range(0, numActivities - 1):            
                inferHoles(activities[i][0], activities[i][1], newActivities, holes, gpsTraces, minSamplingRate)
                inferHoles(activities[i][1], activities[i + 1][0], trips, holes, gpsTraces, minSamplingRate)
        
        # Identify holes in the last activity
        inferHoles(activities[-1][0], activities[-1][1], newActivities, holes, gpsTraces, minSamplingRate)

        # Check if the GPS log ends with a trip
        if activities[-1][-1] < len(gpsTraces) - 2:
            inferHoles(activities[-1][1], len(gpsTraces) - 2, trips, holes, gpsTraces, minSamplingRate)
    
    # If the data comprises a single trip
    else:
        trips.append([0, len(gpsTraces)-1])
    
    return trips, newActivities, holes
        

# Functions that calculate the four features of a GPS point: distance to next point (in meters), 
# time interval (seconds), speed (mph) and acceleration (mph2)

def lengthPoint(gpsTraces, j):
   return calDistance(gpsTraces[j][2:4], gpsTraces[j+1][2:4])

def timePoint(gpsTraces, j):
   return old_div((gpsTraces[j+1][1] - gpsTraces[j][1]), 1000.0)

def speedPoint(gpsTraces, j):
  return 2.23694 * (old_div(float(lengthPoint(gpsTraces, j)), timePoint(gpsTraces, j)))

def accelerationPoint(gpsTraces, j):
  return old_div(abs(speedPoint(gpsTraces, j + 1) - speedPoint(gpsTraces, j)), (old_div(timePoint(gpsTraces,j), 3600.0)))


# Method that that takes as input the list containing GPS data, called gpsTraces, and a tuple containing the 
# indices of the start and end point of a trip, called trip.
#
# The trips are decomposed into their mode chains. 

def inferModeChain(gpsTraces, trip, maxWalkSpeed, maxWalkAcceleration, 
        minSegmentDuration, minSegmentLength, gpsAccuracyThreshold):

    # Step 1: Label GPS points as walk points or non-walk points    
    walkDummy = {}
    i = trip[0]
    while i < trip[1]:
        start, end = i, i
        while end < trip[1] and (gpsTraces[end][4] > gpsAccuracyThreshold 
                or gpsTraces[end + 1][4] > gpsAccuracyThreshold
                or gpsTraces[end + 2][4] > gpsAccuracyThreshold):
            end += 1
        if start == end:
            if speedPoint(gpsTraces, i) < maxWalkSpeed and accelerationPoint(gpsTraces, i) < maxWalkAcceleration:
                walkDummy[i] = 1
            else:
                walkDummy[i] = 0
            i += 1
        else:
            distance = calDistance(gpsTraces[start][2:4], gpsTraces[end][2:4])
            time = old_div((gpsTraces[end][1] - gpsTraces[start][1]), 1000.0)
            speed = 2.23694 * (old_div(float(distance), time))
            dummy = int(speed < maxWalkSpeed)
            while i < end:
                walkDummy[i] = dummy
                i += 1
    #print walkDummy 
    #print
    
    # Step 2: Identify walk and non-walk segments as consecutive walk or non-walk points 
    modeChains = []
    beginSegment = trip[0]
    currentPoint = trip[0] + 1
    while currentPoint < trip[1]:
        if walkDummy[currentPoint] != walkDummy[beginSegment]:
            modeChains.append([beginSegment, currentPoint, int(walkDummy[beginSegment] != 0)])
            beginSegment = currentPoint
        currentPoint += 1
    modeChains.append([beginSegment, currentPoint, int(walkDummy[beginSegment] != 0)])
    #print modeChains
    #print

    # Step 3: If the time span of a segment is greater than minSegmentDuration milliseconds, label it 
    # as certain. If it is less than minSegmentDuration milliseconds, and its backward segment is certain,
    # merge it with the backward segment. If no certain backward segment exists, label the segment as 
    # uncertain, and save it as an independent segment. 
    newModeChains = []
    for i in range(0, len(modeChains)):
        if gpsTraces[modeChains[i][1]][1] - gpsTraces[modeChains[i][0]][1] >= minSegmentDuration:
            modeChains[i].append(1)
            newModeChains.append(modeChains[i])
        elif newModeChains and newModeChains[-1][-1] == 1:
            newModeChains[-1][1] = modeChains[i][1]
        else:
            modeChains[i].append(0)
            newModeChains.append(modeChains[i])
    modeChains = newModeChains
    #print modeChains
    #print

    # Step 4: Merge consecutive uncertain segments into a single certain segment. Calculate average
    # speed over segment and compare it against maxWalkSpeed to determine whether walk or non-walk.
    # Check if this segment exceeds minSegmentDuration milliseconds. If it doesn't, and there exists 
    # a certain forward segment, merge the new segment with this forward segment. 
    newModeChains, i = [modeChains[0][0:-1]], 1
    while i < len(modeChains) and modeChains[i][-1] == 0:
        i += 1
    if i > 1:
        newModeChains[0][1] = modeChains[i-1][1]
        distance = calDistance(gpsTraces[newModeChains[0][0]][2:4], gpsTraces[newModeChains[0][1]][2:4])
        time = old_div((gpsTraces[newModeChains[0][1]][1] - gpsTraces[newModeChains[0][0]][1]), 1000.0)
        speed = 2.23694 * (old_div(float(distance), time))
        newModeChains[0][-1] = int(speed < maxWalkSpeed)
    if i < len(modeChains) and modeChains[0][-1] == 0:
        time = (gpsTraces[newModeChains[0][1]][1] - gpsTraces[newModeChains[0][0]][1])
        if time < minSegmentDuration:
            modeChains[i][0] = trip[0]
            newModeChains = []
    while i < len(modeChains):
        newModeChains.append(modeChains[i][:-1])
        i += 1
    modeChains = newModeChains
    #print modeChains
    #print
        
    # Step 5: Merge consecutive walk segments and consecutive non-walk segments
    newModeChains = [modeChains[0]]
    for i in range(1, len(modeChains)):
        if modeChains[i][2] == newModeChains[-1][2]:
            newModeChains[-1][1] = modeChains[i][1]
        else:
            newModeChains.append(modeChains[i])
    modeChains = newModeChains    

    return modeChains
    

# Method for generating list of dictionary elements, where each elements correspond to an inferred event in the
# last 24 hours for each of the system users

def collect_vij():
    gmtConversion = datetime.datetime.now(pytz.timezone('America/Los_Angeles')).strftime('%z')
    testers = [{'name': 'Andrew', 'ph': '5107259365'},
           {'name': 'Caroline', 'ph': '5107250774'},
           {'name': 'Rory', 'ph': '5107250619'},
           {'name': 'Sreeta', 'ph': '5107250786'},
           {'name': 'Ziheng', 'ph': '5107250744'},
           {'name': 'Vij', 'ph': '5107250740'}]
    db = MongoClient('54.218.218.130').Test_database
    Test_Trips=db.Test_Trips
    Test_Sections=db.Test_Sections

    lastUpdate = datetime.datetime.now().strftime('%Y%m%dT%H%M%S') + gmtConversion
    data = []
    for tester in testers:
        try:
            rawDataFileName = tester['ph'] + '_' + tester['name'] + '_' + lastUpdate + '.txt'
            gpsTraces = getNewGPSData(tester['name'], tester['ph'], lastUpdate, rawDataFileName)
            # print(gpsTraces)
            minDuration, maxRadius, minSamplingRate, gpsAccuracyThreshold = 360000, 50, 300000, 200
            minSeparationDistance, minSeparationTime = 100, 360000
            maxWalkSpeed, maxWalkAcceleration, minSegmentDuration, minSegmentLength = 3.10, 1620, 90000, 200
            trips, activities, holes = inferTripActivity(gpsTraces, minDuration, maxRadius, minSeparationDistance, 
                    minSeparationTime, minSamplingRate, gpsAccuracyThreshold)

            while (trips and activities) or (activities and holes) or (holes and trips):
                event = {}
                user_id=tester['name']
                trip_id=datetime.datetime.fromtimestamp(int(old_div(gpsTraces[trips[0][0]][1],1000))).strftime('%Y%m%dT%H%M%S') + gmtConversion
                eventID = user_id + trip_id
                if ((trips and activities and holes and trips[0][0] < activities[0][0] and trips[0][0] < holes[0][0]) 
                        or (trips and not activities and holes and trips[0][0] < holes[0][0])
                        or (trips and activities and not holes and trips[0][0] < activities[0][0])
                        or (trips and not activities and not holes)):

                    modeChain = inferModeChain(gpsTraces, trips[0], maxWalkSpeed, maxWalkAcceleration, 
                            minSegmentDuration, minSegmentLength, gpsAccuracyThreshold)

                    segmentID, segments = 0, []
                    for mode in modeChain:
                        trackPoints = []
                        for i in range(mode[0], mode[1]):
                            trackPoint = {'Location': {'type': 'Point',
                                                       'coordinates': [gpsTraces[i][3], gpsTraces[i][2]]},
                                          'Time': (datetime.datetime.fromtimestamp(int(old_div(gpsTraces[i][1],1000))).strftime('%Y%m%dT%H%M%S')
                                                        + gmtConversion)}
                            trackPoints.append(trackPoint)

                        if Test_Sections.count_documents({"$and":[ {"user_id":user_id},{"trip_id": trip_id},{"section_id": segmentID}]})==0:
                            sections_todo = {'source':'ITS Berkeley',
                                            'trip_id':trip_id,
                                            'user_id':user_id,
                                            '_id':user_id + datetime.datetime.fromtimestamp(int(old_div(gpsTraces[mode[0]][1],1000))).strftime('%Y%m%dT%H%M%S')
                                                + gmtConversion,
                                            'section_id': segmentID,
                                            'type':'move',
                                            'mode': ((mode[-1] == 0) * 'Non-walk') + ((mode[-1] == 1) * 'Walk'),
                                            'confirmed Mode': '',
                                            'group':'',
                                            'manual':False,
                                            'section_start_time': (datetime.datetime.fromtimestamp(int(old_div(gpsTraces[mode[0]][1],1000))).strftime('%Y%m%dT%H%M%S')
                                                + gmtConversion),
                                            'section_end_time': (datetime.datetime.fromtimestamp(int(old_div(gpsTraces[mode[1]][1],1000))).strftime('%Y%m%dT%H%M%S')
                                                + gmtConversion),
                                            'track_points':trackPoints}

                            Test_Sections.insert(sections_todo)
                            segments.append(sections_todo)
                            segmentID += 1
                    if Test_Trips.count_documents({"$and":[ {"user_id":user_id},{"trip_id": trip_id}]})==0:
                        trips_todo = {'source': 'ITS Berkeley',
                                      'user_id': user_id,
                                      'trip_id':trip_id,
                                      '_id': eventID,
                                      'type': 'move',
                                      'trip_start_time': (datetime.datetime.fromtimestamp(int(old_div(gpsTraces[trips[0][0]][1],1000))).strftime('%Y%m%dT%H%M%S')
                                                    + gmtConversion),
                                      'trip_end_Time': (datetime.datetime.fromtimestamp(int(old_div(gpsTraces[trips[0][1]][1],1000))).strftime('%Y%m%dT%H%M%S')
                                                    + gmtConversion),
                                      'sections': [sections['section_id'] for sections in Test_Sections.find({"$and":[{"user_id":user_id}, {"trip_id":trip_id}]})],
                                      'last_update_time': lastUpdate}
                        Test_Trips.insert(trips_todo)
                        data.append(trips_todo)
                        trips = trips[1:]
            
                elif ((activities and trips and holes and activities[0][0] < trips[0][0] and activities[0][0] < holes[0][0]) 
                        or (activities and not trips and holes and activities[0][0] < holes[0][0])
                        or (activities and trips and not holes and activities[0][0] < trips[0][0])
                        or (activities and not trips and not holes)):
    
                    trackPoints = []
                    for i in range(activities[0][0], activities[0][1]):
                        trackPoint = {'Location': {'type': 'Point',
                                                   'coordinates': [gpsTraces[i][3], gpsTraces[i][2]]},
                                      'Time': (datetime.datetime.fromtimestamp(int(old_div(gpsTraces[i][1],1000))).strftime('%Y%m%dT%H%M%S') 
                                                        + gmtConversion)}
                        trackPoints.append(trackPoint)
                    if Test_Sections.count_documents({"$and":[ {"user_id":user_id},{"trip_id": trip_id}]})==0:
                        sections_todo = {'source':'ITS Berkeley',
                                         'trip_id':trip_id,
                                         'user_id':user_id,
                                         '_id':eventID,
                                         'section_id': 0,
                                         'type':'place',
                                         'section_start_time': (datetime.datetime.fromtimestamp(int(old_div(gpsTraces[activities[0][0]][1],1000))).strftime('%Y%m%dT%H%M%S')
                                                    + gmtConversion),
                                         'section_end_time': (datetime.datetime.fromtimestamp(int(old_div(gpsTraces[activities[0][1]][1],1000))).strftime('%Y%m%dT%H%M%S')
                                                    + gmtConversion),
                                         'track_points' : trackPoints}
                    Test_Sections.insert(sections_todo)
                    if Test_Trips.count_documents({"$and":[ {"user_id":user_id},{"trip_id": trip_id}]})==0:
                        trips_todo = {'source': 'ITS Berkeley',
                                      'user_id': user_id,
                                      'trip_id': trip_id,
                                      '_id': eventID,
                                      'type':'place',
                                      'trip_start_time': (datetime.datetime.fromtimestamp(int(old_div(gpsTraces[activities[0][0]][1],1000))).strftime('%Y%m%dT%H%M%S')
                                                    + gmtConversion),
                                      'trip_end_time': (datetime.datetime.fromtimestamp(int(old_div(gpsTraces[activities[0][1]][1],1000))).strftime('%Y%m%dT%H%M%S')
                                                    + gmtConversion),
                                      'sections': [sections['section_id'] for sections in Test_Sections.find({"$and":[{"user_id":user_id}, {"trip_id":trip_id}]})],
                                      'last_update_time': lastUpdate}
                        Test_Trips.insert(trips_todo)
                        data.append(event)
                        activities = activities[1:]
    
                elif holes:
                    if Test_Trips.count_documents({"$and":[ {"user_id":user_id},{"trip_id": trip_id}]})==0:
                        trips_todo = {'source': 'ITS Berkeley',
                                      'user_id': user_id,
                                      'trip_id': trip_id,
                                      '_id':eventID,
                                      'type': 'hole',
                                      'trip_start_time': (datetime.datetime.fromtimestamp(int(old_div(gpsTraces[holes[0][0]][1],1000))).strftime('%Y%m%dT%H%M%S')
                                                    + gmtConversion),
                                      'trip_end_time': (datetime.datetime.fromtimestamp(int(old_div(gpsTraces[holes[0][1]][1],1000))).strftime('%Y%m%dT%H%M%S')
                                                    + gmtConversion),
                                      'last_update_time': lastUpdate}
                        Test_Trips.insert(trips_todo)
                        data.append(event)
                        holes = holes[1:]
            remove(rawDataFileName)
        except:
            pass
    return data

# This is pretty sucky because we really just want to edit the PYTHONPATH in a separate file.
# But I don't have much patience left right now

if __name__ == "__main__":
  collect_vij()
# Tester personal details, change as appropriate


# Difference in hours between local time and UTC time, remember to change for daylight savings    


# Generate list of events
# data = generateEvents(testers, gmtConversion)
