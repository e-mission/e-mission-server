import logging
from main import carbon
from dao.user import User
import json

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

  renderedTemplate = template("compare.html",
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

def runBackgroundTasks(user_uuid):
  user = User.fromUUID(user_uuid)
  # carbon compare results is a tuple. Tuples are converted to arrays
  # by mongodb
  # In [44]: testUser.setScores(('a','b', 'c', 'd'), ('s', 't', 'u', 'v'))
  # In [45]: testUser.getScore()
  # Out[45]: ([u'a', u'b', u'c', u'd'], [u's', u't', u'u', u'v'])
  carbonCompareResults = carbon.getFootprintCompare(user_uuid)
  setCarbonFootprint(user, carbonCompareResults)

