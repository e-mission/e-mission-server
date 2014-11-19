import logging
from get_database import get_section_db
from main import carbon, common
from datetime import datetime, timedelta
from bottle import template

# Returns the components on which the score is based. These will be combined in
# getScore later, but it is useful to see them to decide how to set up the
# weights

def getScoreComponents(user_uuid):
  user = user_uuid
  now = datetime.now()
  yearago = now - timedelta(days=365)

  # The score is based on the following components:
  # - Percentage of trips classified. We are not auto-classifying high
  # confidence trips, so don't need to handle those here
  pctClassified = common.getClassifiedRatio(user)

  (myModeShareCount, avgModeShareCount,
   myModeShareDistance, avgModeShareDistance,
   myModeCarbonFootprint, avgModeCarbonFootprint,
   myModeCarbonFootprintNoLongMotorized, avgModeCarbonFootprintNoLongMotorized,
   myOptimalCarbonFootprint, avgOptimalCarbonFootprint,
   myOptimalCarbonFootprintNoLongMotorized, avgOptimalCarbonFootprintNoLongMotorized) = carbon.getFootprintCompareForRange(user, yearago, now)

  carbon.delLongMotorizedModes(myModeShareDistance)
  myAllDrive = carbon.getAllDrive(myModeShareDistance)
  myCarbonFootprintSum = sum(myModeCarbonFootprintNoLongMotorized.values())
  myOptimalFootprintSum = sum(myOptimalCarbonFootprintNoLongMotorized.values())
  logging.debug("myCarbonFootprintSum = %s, myOptimalFootprintSum = %s, myAllDrive = %s" %
        (myCarbonFootprintSum, myOptimalFootprintSum, myAllDrive))
  components = [pctClassified,
                 (myCarbonFootprintSum - myOptimalFootprintSum),
                 (myAllDrive - myCarbonFootprintSum),
                 (40.142892 - myCarbonFootprintSum)]
  return components

def getResult(user_uuid):
  components = getScoreComponents(user_uuid)
  renderedTemplate = template("clients/gamified/result_template.html",
                              pctClassified = components[0],
                              mineMinusOptimal = components[1],
                              allDriveMinusMine = components[2])
  return renderedTemplate

# These are copy/pasted from our first client, the carshare study
def getSectionFilter(uuid):
  # We are not planning to do any filtering for this study. Bring on the worst!
  return []

def clientSpecificSetters(uuid, sectionId, predictedModeMap):
  return None

def getClientConfirmedModeField():
  return None
