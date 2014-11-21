import logging
from get_database import get_section_db
from main import carbon, common
from datetime import datetime, timedelta

# sb375 is a weekly goal - we convert it to daily by dividing by 7
sb375DailyGoal = 40.142892/7

# Returns the components on which the score is based. These will be combined in
# getScore later, but it is useful to see them to decide how to set up the
# weights

def getScoreComponents(user_uuid, start, end):
  # The score is based on the following components:
  # - Percentage of trips classified. We are not auto-classifying high
  # confidence trips, so don't need to handle those here
  user = user_uuid

  pctClassified = common.getClassifiedRatio(user, start, end)

  (myModeShareCount, avgModeShareCount,
   myModeShareDistance, avgModeShareDistance,
   myModeCarbonFootprint, avgModeCarbonFootprint,
   myModeCarbonFootprintNoLongMotorized, avgModeCarbonFootprintNoLongMotorized,
   myOptimalCarbonFootprint, avgOptimalCarbonFootprint,
   myOptimalCarbonFootprintNoLongMotorized, avgOptimalCarbonFootprintNoLongMotorized) = carbon.getFootprintCompareForRange(user, start, end)

  carbon.delLongMotorizedModes(myModeShareDistance)
  myAllDrive = carbon.getAllDrive(myModeShareDistance)
  myCarbonFootprintSum = sum(myModeCarbonFootprintNoLongMotorized.values())
  myOptimalFootprintSum = sum(myOptimalCarbonFootprintNoLongMotorized.values())
  logging.debug("myCarbonFootprintSum = %s, myOptimalFootprintSum = %s, myAllDrive = %s" %
        (myCarbonFootprintSum, myOptimalFootprintSum, myAllDrive))
  handleZero = lambda x, y: 0 if y == 0 else float(x)/y
  components = [pctClassified,
                handleZero(myCarbonFootprintSum - myOptimalFootprintSum, myOptimalFootprintSum),
                handleZero(myAllDrive - myCarbonFootprintSum, myAllDrive),
                handleZero(sb375DailyGoal - myCarbonFootprintSum, sb375DailyGoal)]
  return components

def calcScore(componentArr):
  [pctClassified, mineMinusOptimal, allDriveMinusMine] = componentArr
  # We want the ratio between the three components to be 5 : 3 : 2
  # Let's just convert everything to percentages to keep the ratios consistent
  # Also, we subtract the mineMinusOptimal term, since being way above optimal
  # should lower your score
  return 5 * pctClassified + 3 * allDriveMinusMine - 2 * mineMinusOptimal

def getResult(user_uuid):
  # This is in here, as opposed to the top level as recommended by the PEP
  # because then we don't have to worry about loading bottle in the unit tests
  from bottle import template

  now = datetime.now()
  yearago = now - timedelta(days=365)

  components = getScoreComponents(user_uuid, yearago, now)
  [pctClassified, mineMinusOptimal, allDriveMinusMine] = components
  score = calcScore(components)
  renderedTemplate = template("clients/gamified/result_template.html",
                              pctClassified = pctClassified,
                              mineMinusOptimal = mineMinusOptimal,
                              allDriveMinusMine = allDriveMinusMine,
                              sb375MinusMine = sb375MinusMine)
  return renderedTemplate

# These are copy/pasted from our first client, the carshare study
def getSectionFilter(uuid):
  # We are not planning to do any filtering for this study. Bring on the worst!
  return []

def clientSpecificSetters(uuid, sectionId, predictedModeMap):
  return None

def getClientConfirmedModeField():
  return None
