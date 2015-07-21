# Standard imports
import logging
import time as systime
from datetime import datetime, time, timedelta
import json

# Our imports
import emission.analysis.result.carbon as carbon
import emission.net.api.stats as stats
from emission.core.wrapper.user import User

# BEGIN: Code to get and set client specific fields in the profile (currentScore and previousScore)
def getCarbonFootprint(user):
    profile = user.getProfile()
    if profile is None:
        return None
    return profile.get("carbon_footprint")

def setCarbonFootprint(user, newFootprint):
    user.setClientSpecificProfileFields({'carbon_footprint': newFootprint})
# END: Code to get and set client specific fields in the profile (currentScore and previousScore)

def getResult(user_uuid):
  # This is in here, as opposed to the top level as recommended by the PEP
  # because then we don't have to worry about loading bottle in the unit tests
  from bottle import template

  user = User.fromUUID(user_uuid)
  currFootprint = getCarbonFootprint(user)

  if currFootprint is None:
    currFootprint = carbon.getFootprintCompare(user_uuid)
    setCarbonFootprint(user, currFootprint)

  (myModeShareCount, avgModeShareCount,
     myModeShareDistance, avgModeShareDistance,
     myModeCarbonFootprint, avgModeCarbonFootprint,
     myModeCarbonFootprintNoLongMotorized, avgModeCarbonFootprintNoLongMotorized, # ignored
     myOptimalCarbonFootprint, avgOptimalCarbonFootprint,
     myOptimalCarbonFootprintNoLongMotorized, avgOptimalCarbonFootprintNoLongMotorized) = currFootprint

  renderedTemplate = template("clients/data/result_template.html",
                      myModeShareCount = json.dumps(myModeShareCount),
                      avgModeShareCount = json.dumps(avgModeShareCount),
                      myModeShareDistance = json.dumps(myModeShareDistance),
                      avgModeShareDistance = json.dumps(avgModeShareDistance),
                      myModeCarbonFootprint = json.dumps(myModeCarbonFootprint),
                      avgModeCarbonFootprint = json.dumps(avgModeCarbonFootprint),
                      myOptimalCarbonFootprint = json.dumps(myOptimalCarbonFootprint),
                      avgOptimalCarbonFootprint = json.dumps(avgOptimalCarbonFootprint))
                  
  # logging.debug(renderedTemplate)
  return renderedTemplate

def getCategorySum(carbonFootprintMap):
    return sum(carbonFootprintMap.values())

# TODO: Change the use of runBackgroundTasks 
def runBackgroundTasks(user_uuid):
  today = datetime.now()
  runBackgroundTasksForDay(user_uuid, today)

def runBackgroundTasksForDay(user_uuid, today):
  today_dt = datetime.combine(today, time.max)
  user = User.fromUUID(user_uuid)

  # carbon compare results is a tuple. Tuples are converted to arrays
  # by mongodb
  # In [44]: testUser.setScores(('a','b', 'c', 'd'), ('s', 't', 'u', 'v'))
  # In [45]: testUser.getScore()
  # Out[45]: ([u'a', u'b', u'c', u'd'], [u's', u't', u'u', u'v'])
  weekago = today_dt - timedelta(days=7)
  carbonCompareResults = carbon.getFootprintCompareForRange(user_uuid, weekago, today_dt)
  setCarbonFootprint(user, carbonCompareResults)

  (myModeShareCount, avgModeShareCount,
     myModeShareDistance, avgModeShareDistance,
     myModeCarbonFootprint, avgModeCarbonFootprint,
     myModeCarbonFootprintNoLongMotorized, avgModeCarbonFootprintNoLongMotorized, # ignored
     myOptimalCarbonFootprint, avgOptimalCarbonFootprint,
     myOptimalCarbonFootprintNoLongMotorized, avgOptimalCarbonFootprintNoLongMotorized) = carbonCompareResults
  # We only compute server stats in the background, because including them in
  # the set call means that they may be invoked when the user makes a call and
  # the cached value is None, which would potentially slow down user response time
  msNow = systime.time()
  stats.storeResultEntry(user_uuid, stats.STAT_MY_CARBON_FOOTPRINT, msNow, getCategorySum(myModeCarbonFootprint))
  stats.storeResultEntry(user_uuid, stats.STAT_MY_CARBON_FOOTPRINT_NO_AIR, msNow, getCategorySum(myModeCarbonFootprintNoLongMotorized))
  stats.storeResultEntry(user_uuid, stats.STAT_MY_OPTIMAL_FOOTPRINT, msNow, getCategorySum(myOptimalCarbonFootprint))
  stats.storeResultEntry(user_uuid, stats.STAT_MY_OPTIMAL_FOOTPRINT_NO_AIR, msNow, getCategorySum(myOptimalCarbonFootprintNoLongMotorized))
  stats.storeResultEntry(user_uuid, stats.STAT_MY_ALLDRIVE_FOOTPRINT, msNow, getCategorySum(myModeShareDistance) * (278.0/(1609 * 1000)))
  stats.storeResultEntry(user_uuid, stats.STAT_MEAN_FOOTPRINT, msNow, getCategorySum(avgModeCarbonFootprint))
  stats.storeResultEntry(user_uuid, stats.STAT_MEAN_FOOTPRINT_NO_AIR, msNow, getCategorySum(avgModeCarbonFootprintNoLongMotorized))
