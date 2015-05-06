import logging
from get_database import get_trip_db, get_section_db, get_alternatives_db #DEMO
from main import carbon, common, stats
from datetime import datetime, time, timedelta
from dao.user import User
import math
import json
from uuid import UUID
import time
from clients.gamified import gamified
from clients.recommendation import recommendation
from clients.data import data

# TODO: Consider subclassing to provide client specific user functions
def setCurrView(uuid, newView):
  user = User.fromUUID(uuid)
  user.setClientSpecificProfileFields({'curr_view': newView})
  stats.storeResultEntry(uuid, stats.STAT_VIEW_CHOICE, time.time(), newView)

def getCurrView(uuid):
  user = User.fromUUID(uuid)
  profile = user.getProfile()
  if profile is None:
    logging.debug("profile is None, returning data")
    return "data"
  logging.debug("profile.get('curr_view', 'dummy') is %s" % profile.get("curr_view", "data"))
  return profile.get("curr_view", "data")

def switchResultDisplay(params):
  logging.debug("params = %s" % (params))

  uuid = UUID(params['uuid'])
  newView = params['new_view']
  logging.debug("Changing choice for user %s to %s" % (uuid, newView))
  setCurrView(uuid, newView)
  # TODO: Add stats about the switch as part of the final stats-adding pass
  return {'curr_view': newView }

def getResult(user_uuid):
  # This is in here, as opposed to the top level as recommended by the PEP
  # because then we don't have to worry about loading bottle in the unit tests
  from bottle import template
  import base64
  from dao.user import User
  from dao.client import Client

  user_uuid = "6433c8cf-c4c5-3741-9144-5905379ece6e"
  user = User.fromUUID(user_uuid)

  # renderedTemplate = template("clients/choice/result_template.html",
  #                         variables = json.dumps({'curr_view': getCurrView(user_uuid),
  #                                      'uuid': str(user_uuid),
  #                                      'client_key': Client("choice").getClientKey()}),
  #                         gameResult = base64.b64encode(gamified.getResult(user_uuid)),
  #                         dataResult = base64.b64encode(default.getResult(user_uuid)),
  #                         recommendationResult = base64.b64encode(recommendation.getResult(user_uuid)))
  # return renderedTemplate

  # DEMO
  originalTrip = get_trip_db().find_one({'user_id': UUID(user.uuid), 'recommended_alternative': {'$exists': True} })
  recommendedTrip = originalTrip['recommended_alternative']
  originalSections = list(get_section_db().find({ 'trip_id': originalTrip['trip_id'] }))

  renderedTemplate = template("clients/recommendation/result_template.html",
                              recommendedTrip = recommendedTrip,
                              originalTrip = originalTrip,
                              originalSections = originalSections)

  return renderedTemplate

# These are copy/pasted from our first client, the carshare study
def getSectionFilter(uuid):
  # We are not planning to do any filtering for this study. Bring on the worst!
  return []

def clientSpecificSetters(uuid, sectionId, predictedModeMap):
  return None

def getClientConfirmedModeField():
  return None

# TODO: Simplify this. runBackgroundTasks are currently only invoked from the
# result precomputation code. We could change that code to pass in the day, and
# remove this interface. Extra credit: should we pass in the day, or a date
# range?  Passing in the date range could make it possible for us to run the
# scripts more than once a day...
def runBackgroundTasks(uuid):
  today = datetime.now().date()
  runBackgroundTasksForDay(uuid, today)

def runBackgroundTasksForDay(uuid, today):
  gamified.runBackgroundTasksForDay(uuid, today)
  default.runBackgroundTasksForDay(uuid, today)
