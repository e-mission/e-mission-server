import logging
from get_database import get_section_db
from main import carbon, common
from datetime import datetime, time, timedelta
from dao.user import User

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
  [pctClassified, mineMinusOptimal, allDriveMinusMine, sb375DailyGoal] = componentArr
  # We want the ratio between the three components to be 5 : 3 : 2
  # Let's just convert everything to percentages to keep the ratios consistent
  # Also, we subtract the mineMinusOptimal term, since being way above optimal
  # should lower your score
  return 5 * pctClassified + 3 * allDriveMinusMine - 2 * mineMinusOptimal + 1 * sb375DailyGoal

def getScore(user_uuid, start, end):
    components = getScoreComponents(user_uuid, start, end)
    return calcScore(components)

# Ok so this is a big tricky to get right.
# We want this to be in increments of a day, so that the SB375 target
# calculation makes sense.
# The high level plan is to run it around midnight everyday and update the
# score based on the day that just passed. But what exactly does "around
# midnight" mean?
# It can't be just before midnight, because then we will skip trips around midnight
# I guess we start right after midnight
# Couple of other challenges:
# - What to do about trips that span days?
# - What to do about people in other time zones who have a different midnight?
# In order to handle both of these, we should really have task scheduled from
# the app instead of a cronjob, and have it track the last time it was run. But
# let's do the cheap solution for now so that we know whether it works at all.
def updateScore(user_uuid):
    today = datetime.now().date()
    yesterday = today - timedelta(days = 1)
    yesterdayStart = datetime.combine(yesterday, time.min)
    todayStart = datetime.combine(today, time.min)

    user = User.fromUUID(user_uuid)
    # TODO: getScore() shouldn't really be defined in User because it doesn't apply to all clients.
    # Need to figure out how to structure client specific profile enhancements.
    # Should they even be stored in the user profile?
    newScore = user.getScore() + getScore(user_uuid, yesterdayStart, todayStart)
    user.setScore(newScore)

def getResult(user_uuid):
  # This is in here, as opposed to the top level as recommended by the PEP
  # because then we don't have to worry about loading bottle in the unit tests
  from bottle import template

  score = user.fromUUID(user_uuid).getScore()
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
