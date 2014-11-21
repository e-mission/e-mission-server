import logging
import distance
from datetime import datetime, timedelta
# from get_database import get_user_db
from common import getDistinctUserCount, getAllModes, getDisplayModes, getQuerySpec, addFilterToSpec, getTripCountForMode, getModeShare, getDistanceForMode,\
    getModeShareDistance, convertToAvg

# Although air is a motorized mode, we don't include it here because there is
# not much point in finding < 5 km air trips to convert to non motorized trips
# Instead, we handle air separately
motorizedModeList = ["bus", "train", "car"]
longMotorizedModeList = ["air"]

carbonFootprintForMode = {'walking' : 0,
                          'running' : 0,
                          'cycling' : 0,
                            'mixed' : 0,
                        'bus_short' : 267.0/1609,
                         'bus_long' : 267.0/1609,
                      'train_short' : 92.0/1609,
                       'train_long' : 92.0/1609,
                        'car_short' : 278.0/1609,
                         'car_long' : 278.0/1609,
                        'air_short' : 217.0/1609,
                         'air_long' : 217.0/1609
                      }

# TODO: What should the optimal carbon footprint for air travel be?  One option
# is to say that it should be replaced by a train ride just like all motorized
# transport. But that is not really practical for overseas trips. Punt it for
# now and just assume it is optimal? But some car trips might not be
# replaceable by car trips anyway. Ok so punt it and replace with train 
optimalCarbonFootprintForMode = {'walking' : 0,
                                 'running' : 0,
                                 'cycling' : 0,
                                   'mixed' : 0,
                               'bus_short' : 0,
                                'bus_long' : 92.0/1609,
                             'train_short' : 0,
                              'train_long' : 92.0/1609,
                               'car_short' : 0,
                                'car_long' : 92.0/1609,
                               'air_short' : 92.0/1609,
                                'air_long' : 217.0/1609
                            }

# TODO: We need to figure out whether to pass in mode or modeID
def getModeCarbonFootprint(user, carbonFootprintMap,start,end):
  modeDistanceMap = getShortLongModeShareDistance(user,start,end)
  logging.debug("getModeCarbonFootprint, modeDistanceMap = %s" % modeDistanceMap)
  logging.debug("footprintMap = %s" % carbonFootprintMap)
  return getCarbonFootprintsForMap(modeDistanceMap, carbonFootprintMap)

def getCarbonFootprintsForMap(modeDistanceMap, carbonFootprintMap):
  logging.debug("In getCarbonFootprintsForMap, modeDistanceMap = %s" % modeDistanceMap)
  modeFootprintMap = {}
  for modeName in modeDistanceMap:
    # logging.debug("Consider mode with name %s" % modeName)
    carbonForMode = float(carbonFootprintMap[modeName] * modeDistanceMap[modeName])/1000
    # logging.debug("carbonForMode %s = %s from %s * %s" % 
    #     (modeName, carbonForMode, carbonFootprintMap[modeName], modeDistanceMap[modeName]))
    modeFootprintMap[modeName] = carbonForMode
  return modeFootprintMap

def getShortLongModeShareDistance(user,start,end):
  displayModes = getDisplayModes()
  modeDistanceMap = {}
  for mode in displayModes:
    modeId = mode['mode_id']
    if mode['mode_name'] in (motorizedModeList + longMotorizedModeList):
      # We need to split it into short and long
      if mode['mode_name'] in motorizedModeList:
        specShort = appendDistanceFilter(getQuerySpec(user, modeId,start,end), {"$lte": 5000})
        specLong = appendDistanceFilter(getQuerySpec(user, modeId,start,end), {"$gte": 5000})
      else:
        assert(mode['mode_name'] in longMotorizedModeList)
        threshold = 600 * 1000 # 600km in meters
        specShort = appendDistanceFilter(getQuerySpec(user, modeId,start,end), {"$lte": threshold})
        specLong = appendDistanceFilter(getQuerySpec(user, modeId,start,end), {"$gte": threshold})
      shortDistanceForMode = getDistanceForMode(specShort)
      modeDistanceMap[mode['mode_name']+"_short"] = shortDistanceForMode
      longDistanceForMode = getDistanceForMode(specLong)
      modeDistanceMap[mode['mode_name']+"_long"] = longDistanceForMode
    else:
      spec = getQuerySpec(user, mode['mode_id'],start,end)
      distanceForMode = getDistanceForMode(spec)
      modeDistanceMap[mode['mode_name']] = distanceForMode
  return modeDistanceMap

def appendDistanceFilter(spec, distFilter):
  distanceFilter = {'distance': distFilter}
  return addFilterToSpec(spec, distanceFilter)

def delModeNameWithSuffix(modeName, prefix, modeDistanceMap):
  modeNameWithSuffix = '%s%s' % (modeName, prefix)
  logging.debug("In delModeNameWithSuffix.modeNameWithSuffix = %s" % modeNameWithSuffix)
  if modeNameWithSuffix in modeDistanceMap:
    del modeDistanceMap[modeNameWithSuffix]

# Doesn't return anything, deletes entries from the distance map as a side effect
def delLongMotorizedModes(modeDistanceMap):
  logging.debug("At the beginning of delLongMotorizedModes, the distance map was %s" % modeDistanceMap)
  for mode in longMotorizedModeList:
      logging.debug("Deleting entries for mode %s from the distance map" % mode)
      delModeNameWithSuffix(mode, "", modeDistanceMap)
      delModeNameWithSuffix(mode, "_short", modeDistanceMap)
      delModeNameWithSuffix(mode, "_long", modeDistanceMap)
  logging.debug("At the end of delLongMotorizedModes, the distance map was %s" % modeDistanceMap)

def getFootprintCompare(user):
  now = datetime.now()
  weekago = now - timedelta(days=7)
  return getFootprintCompareForRange(user, weekago, now)

def getFootprintCompareForRange(user, start, end):
  myModeShareCount = getModeShare(user, start,end)
  totalModeShareCount = getModeShare(None, start,end)
  logging.debug("myModeShareCount = %s totalModeShareCount = %s" %
      (myModeShareCount, totalModeShareCount))

  myModeShareDistance = getModeShareDistance(user,start,end)
  totalModeShareDistance = getModeShareDistance(None, start,end)
  logging.debug("myModeShareDistance = %s totalModeShareDistance = %s" %
      (myModeShareDistance, totalModeShareDistance))
  myShortLongModeShareDistance = getShortLongModeShareDistance(user, start, end)
  totalShortLongModeShareDistance = getShortLongModeShareDistance(None, start, end)

  myModeCarbonFootprint = getCarbonFootprintsForMap(myShortLongModeShareDistance, carbonFootprintForMode)
  totalModeCarbonFootprint = getCarbonFootprintsForMap(totalShortLongModeShareDistance, carbonFootprintForMode)
  logging.debug("myModeCarbonFootprint = %s, totalModeCarbonFootprint = %s" %
      (myModeCarbonFootprint, totalModeCarbonFootprint))

  myOptimalCarbonFootprint = getCarbonFootprintsForMap(myShortLongModeShareDistance, optimalCarbonFootprintForMode)
  totalOptimalCarbonFootprint = getCarbonFootprintsForMap(totalShortLongModeShareDistance, optimalCarbonFootprintForMode)
  logging.debug("myOptimalCarbonFootprint = %s, totalOptimalCarbonFootprint = %s" %
      (myOptimalCarbonFootprint, totalOptimalCarbonFootprint))

  delLongMotorizedModes(myShortLongModeShareDistance)
  delLongMotorizedModes(totalShortLongModeShareDistance)
  logging.debug("After deleting long motorized mode, map is %s", myShortLongModeShareDistance)

  myModeCarbonFootprintNoLongMotorized = getCarbonFootprintsForMap(myShortLongModeShareDistance, carbonFootprintForMode)
  totalModeCarbonFootprintNoLongMotorized = getCarbonFootprintsForMap(totalShortLongModeShareDistance, carbonFootprintForMode)
  myOptimalCarbonFootprintNoLongMotorized = getCarbonFootprintsForMap(myShortLongModeShareDistance, optimalCarbonFootprintForMode)
  totalOptimalCarbonFootprintNoLongMotorized = getCarbonFootprintsForMap(totalShortLongModeShareDistance, optimalCarbonFootprintForMode)

  nUsers = getDistinctUserCount(getQuerySpec(None, None, start, end))
  # Hack to prevent divide by zero on an empty DB.
  # We will never really have an empty DB in the real production world,
  # but shouldn't crash in that case.
  # This is pretty safe because if we have no users, we won't have any modeCarbonFootprint either
  if nUsers == 0:
    nUsers = 1

  avgModeShareCount = convertToAvg(totalModeShareCount, nUsers)
  avgModeShareDistance = convertToAvg(totalModeShareDistance, nUsers)
  avgModeCarbonFootprint = convertToAvg(totalModeCarbonFootprint, nUsers)
  avgModeCarbonFootprintNoLongMotorized = convertToAvg(totalModeCarbonFootprintNoLongMotorized, nUsers)
  avgOptimalCarbonFootprint = convertToAvg(totalModeCarbonFootprint, nUsers)
  avgOptimalCarbonFootprintNoLongMotorized = convertToAvg(totalModeCarbonFootprintNoLongMotorized, nUsers)
 
#   avgCarbonFootprint = totalCarbonFootprint/nUsers
# 
#   carbonFootprint = {"mine": myCarbonFootprint,
#          "mean": avgCarbonFootprint,
#          "2005 avg": 47173.568,
#          "2020 target": 43771.628,
#          "2035 target": 40142.892}

  return (myModeShareCount, avgModeShareCount,
          myModeShareDistance, avgModeShareDistance,
          myModeCarbonFootprint, avgModeCarbonFootprint,
          myModeCarbonFootprintNoLongMotorized, avgModeCarbonFootprintNoLongMotorized,
          myOptimalCarbonFootprint, avgOptimalCarbonFootprint,
          myOptimalCarbonFootprintNoLongMotorized, avgOptimalCarbonFootprintNoLongMotorized)

def getSummaryAllTrips(start,end):
  # totalModeShareDistance = getModeShareDistance(None, start, end)
  totalShortLongModeShareDistance = getShortLongModeShareDistance(None, start, end)

  totalModeCarbonFootprint = getCarbonFootprintsForMap(totalShortLongModeShareDistance,
      carbonFootprintForMode)
  totalOptimalCarbonFootprint = getCarbonFootprintsForMap(totalShortLongModeShareDistance,
      optimalCarbonFootprintForMode)

  # Hack to prevent divide by zero on an empty DB.
  # We will never really have an empty DB in the real production world,
  # but shouldn't crash in that case.
  # This is pretty safe because if we have no users, we won't have any modeCarbonFootprint either
  nUsers = getDistinctUserCount(getQuerySpec(None, None, start, end))
  if nUsers == 0:
    nUsers = 1
  sumModeCarbonFootprint = sum(totalModeCarbonFootprint.values())
  sumOptimalCarbonFootprint = sum(totalOptimalCarbonFootprint.values())
  sumModeShareDistance = sum(totalShortLongModeShareDistance.values())/1000

  # We need to calculate the sums before we delete certain modes from the mode share dict
  delLongMotorizedModes(totalShortLongModeShareDistance)
  logging.debug("After deleting long motorized mode, map is %s", totalShortLongModeShareDistance)

  totalModeCarbonFootprintNoLongMotorized = getCarbonFootprintsForMap(
        totalShortLongModeShareDistance,
        carbonFootprintForMode)
  totalOptimalCarbonFootprintNoLongMotorized = getCarbonFootprintsForMap(
        totalShortLongModeShareDistance,
        optimalCarbonFootprintForMode)
  return {
          "current": float(sumModeCarbonFootprint)/nUsers,
          "optimal": float(sumOptimalCarbonFootprint)/nUsers,
          "current no air": float(sum(totalModeCarbonFootprintNoLongMotorized.values()))/nUsers,
          "optimal no air": float(sum(totalOptimalCarbonFootprintNoLongMotorized.values()))/nUsers,
          "all drive": float((sumModeShareDistance * carbonFootprintForMode['car_short']))/nUsers,
          "SB375 mandate for 2035": 40.142892,
          "EO 2050 goal (80% below 1990)": 8.28565
         }

def getAllDrive(modeDistanceMap):
   totalDistance = sum(modeDistanceMap.values()) / 1000
   return totalDistance * carbonFootprintForMode['car_short']
