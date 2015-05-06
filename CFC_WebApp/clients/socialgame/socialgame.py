import logging
from get_database import get_section_db
from main import carbon, common, stats
from datetime import datetime, timedelta
from datetime import time as dttime
import time
from dao.user import User
import math
from get_database import get_uuid_db
from main import userclient

# sb375 is a weekly goal - we convert it to daily by dividing by 7
sb375DailyGoal = 40.142892/7

# BEGIN: Code to get and set client specific fields in the profile (currentScore and previousScore)
def getStoredScore(user):
    profile = user.getProfile()
    if profile is None:
        return (0, 0)
    currScore = profile.get('currentScore', 0)
    prevScore = profile.get('previousScore', 0)
    return (prevScore, currScore)

def setScores(user, prevScore, newScore):
    logging.debug("Changing score for user %s from %s to %s" % (user.uuid, prevScore, newScore))
    user.setClientSpecificProfileFields({'previousScore': prevScore,
                                         'currentScore': newScore})
    # TODO: Add a server side stat here so that we can know for sure how the score varies over time
# END: Code to get and set client specific fields in the profile (currentScore and previousScore)

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
  return 50 * pctClassified + 30 * allDriveMinusMine - 20 * mineMinusOptimal + 10 * sb375DailyGoal

def getScore(user_uuid, start, end):
    components = getScoreComponents(user_uuid, start, end)
    [pctClassified, mineMinusOptimal, allDriveMinusMine, sb375DailyGoal] = components
    stats.storeResultEntry(user_uuid, stats.STAT_PCT_CLASSIFIED, time.time(), pctClassified)
    stats.storeResultEntry(user_uuid, stats.STAT_MINE_MINUS_OPTIMAL, time.time(), mineMinusOptimal)
    stats.storeResultEntry(user_uuid, stats.STAT_ALL_DRIVE_MINUS_MINE, time.time(), allDriveMinusMine)
    stats.storeResultEntry(user_uuid, stats.STAT_SB375_DAILY_GOAL, time.time(), sb375DailyGoal)
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
    updateScoreForDay(user_uuid, today)

def updateScoreForDay(user_uuid, today):
    pass
    yesterday = today - timedelta(days = 1)
    dayBeforeYesterday = today - timedelta(days = 2)

    dayBeforeYesterdayStart = datetime.combine(dayBeforeYesterday, dttime.min)
    yesterdayStart = datetime.combine(yesterday, dttime.min)
    todayStart = datetime.combine(today, dttime.min)

    user = User.fromUUID(user_uuid)
    (discardedScore, prevScore) = getStoredScore(user)
    # Using score from dayBeforeYesterday instead of yesterday because there is
    # currently a significant lag in the time for e-mission to prompt for
    # entries, so people might not confirm yesterday's trips until sometime
    # today, which means that it won't be counted in their score
    newScore = prevScore + getScore(user_uuid, dayBeforeYesterdayStart, yesterdayStart)
    if newScore < 0:
        newScore = 0
    stats.storeResultEntry(user_uuid, stats.STAT_GAME_SCORE, time.time(), newScore)
    setScores(user, prevScore, newScore)

def getLevel(score):
  if score < 100:
    level = 1
    sublevel = math.floor(score / 20) + 1
  elif score < 1000:
    level = 2
    sublevel = math.floor(score / 200) + 1
  elif score < 10000:
    level = 3
    sublevel = math.floor(score / 2000) + 1
  else:
    # Off the charts, stay at the top image
    level = 3
    sublevel = 5
  return (level, sublevel)

def getFileName(level, sublevel):
    return "level_%s_%s.png" % (int(level), int(sublevel))

def getResult(user_uuid):
  # This is in here, as opposed to the top level as recommended by the PEP
  # because then we don't have to worry about loading bottle in the unit tests
  from bottle import template

  (prevScore, currScore) = getStoredScore(User.fromUUID(user_uuid))
  (level, sublevel) = getLevel(currScore)
  
  renderedTemplate = template("clients/socialgame/result_template.html",
                              level_picture_filename = getFileName(level, sublevel),
                              prevScore = prevScore,
                              currScore = currScore)
  return renderedTemplate

# These are copy/pasted from our first client, the carshare study
def getSectionFilter(uuid):
  # We are not planning to do any filtering for this study. Bring on the worst!
  return []

def clientSpecificSetters(uuid, sectionId, predictedModeMap):
  return None

def getClientConfirmedModeField():
  return None

def runBackgroundTasks(uuid):
  today = datetime.now().date()
  runBackgroundTasksForDay(uuid, today)

def runBackgroundTasksForDay(uuid, today):
  updateScoreForDay(uuid, today)
