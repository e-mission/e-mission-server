import logging
from get_database import get_section_db
from bottle import template

def classifiedCount(request):
  print "carshare.classifiedCount called for user %s!" % request['user']
  return getResult(request['user'])

def getResult(user_uuid):
  logging.debug("carshare.getResult called for user %s!" % user_uuid)
  tripCount = get_section_db().find({"$and": [{'user_id': user_uuid}, {'type': 'move'}, {'confirmed_mode': {'$ne': ''}}]}).count()
  renderedTemplate = template("clients/carshare/result_template.html",
                              count = tripCount)
  return renderedTemplate

def getSectionFilter(uuid):
  from dao.user import User
  from datetime import datetime, timedelta

  logging.info("carshare.getSectionFilter called for user %s" % uuid)
  # If this is the first two weeks, show everything
  user = User.fromUUID(uuid)
  # Note that this is the last time that the profile was updated. So if the
  # user goes to the "Auth" screen and signs in again, it will be updated, and
  # we will reset the clock. If this is not acceptable, we need to ensure that
  # we have a create ts that is never updated
  updateTS = user.getUpdateTS()
  if (datetime.now() - updateTS) < timedelta(days = 14):
    # In the first two weeks, don't do any filtering
    return []
  else:
    return [{'auto_confirmed.prob': {'$lt': 90}}]

def clientSpecificSetters(uuid, sectionId, predictedModeMap):
  from main import common
  from get_database import get_mode_db

  maxMode = None
  maxProb = 0
  for mode, prob in predictedModeMap.iteritems():
    logging.debug("Considering mode %s and prob %s" % (mode, prob))
    if prob > maxProb:
      maxProb = prob
      maxMode = mode
  logging.debug("maxMode = %s, maxProb = %s" % (maxMode, maxProb))
  return {"$set":
            {"auto_confirmed": {
                "mode": common.convertModeNameToIndex(get_mode_db(), maxMode),
                "prob": maxProb,
              }
            }
         }

def getClientConfirmedModeField():
    return "auto_confirmed.mode"
