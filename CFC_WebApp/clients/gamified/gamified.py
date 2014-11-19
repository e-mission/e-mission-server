import logging
from get_database import get_section_db
from main import carbon, common

def getScore(request):
  user = request['user']

  # The score is based on the following components:
  # - Percentage of trips classified. We are not auto-classifying high
  # confidence trips, so don't need to handle those here
  pctClassified = common.getClassifiedRatio(user)

  (myModeShareCount, avgModeShareCount,
   myModeShareDistance, avgModeShareDistance,
   myModeCarbonFootprint, avgModeCarbonFootprint,
   myModeCarbonFootprintNoLongMotorized, avgModeCarbonFootprintNoLongMotorized,
   myOptimalCarbonFootprint, avgOptimalCarbonFootprint,
   myOptimalCarbonFootprintNoLongMotorized, avgOptimalCarbonFootprintNoLongMotorized) = carbon.getFootprintCompare(user)

  carbon.delLongMotorizedModes(myModeShareDistance)
  myAllDrive = carbon.getAllDrive(myModeShareDistance)
  myCarbonFootprintSum = sum(myModeCarbonFootprintNoLongMotorized.values())
  myOptimalFootprintSum = sum(myOptimalCarbonFootprintNoLongMotorized.values())
  components = [pctClassified,
                 (myCarbonFootprintSum - myOptimalFootprintSum),
                 (myAllDrive - myCarbonFootprintSum),
                 (40.142892 - myCarbonFootprintSum)]
  return components

# These are copy/pasted from our first client, the carshare study
def getSectionFilter(uuid):
  # We are not planning to do any filtering for this study. Bring on the worst!
  return []

def clientSpecificSetters(uuid, sectionId, predictedModeMap):
  return None

def getClientConfirmedModeField():
  return None
