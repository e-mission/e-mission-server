import logging

def classifiedCount(request):
  print "testclient.classifiedCount called for user %s!" % request['user']
  return {'count': 0}

# These are copy/pasted from our first client, the carshare study
def getSectionFilter(uuid):
  from dao.user import User
  from datetime import datetime, timedelta

  logging.info("testclient.getSectionFilter called for user %s" % uuid)
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
    return [{'test_auto_confirmed.prob': {'$lt': 0.9}}]

def clientSpecificSetters(uuid, sectionId, predictedModeMap):
  from main import common
  from get_database import get_mode_db

  maxMode = None
  maxProb = 0
  for mode, prob in predictedModeMap.iteritems():
    print "Considering mode %s and prob %s" % (mode, prob)
    if prob > maxProb:
      maxProb = prob
      maxMode = mode
  print "maxMode = %s, maxProb = %s" % (mode, prob)
  return {"$set":
            {"test_auto_confirmed": {
                "mode": common.convertModeNameToIndex(get_mode_db(), maxMode),
                "prob": maxProb,
              }
            }
         }

def getClientConfirmedModeField():
  return "test_auto_confirmed.mode"

def runBackgroundTasks(uuid):
  from dao.user import User

  testuser = User.fromUUID(uuid)
  testuser.setClientSpecificProfileFields({'testfield1': 'value1', 'testfield2': 'value2'})

