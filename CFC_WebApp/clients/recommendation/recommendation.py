import logging
from main import carbon, stats
from dao.user import User
import time as systime
from datetime import datetime, time, timedelta
import json

# BEGIN: Code to get and set client specific fields in the profile (currentScore and previousScore)
# END: Code to get and set client specific fields in the profile (currentScore and previousScore)

def getResult(user_uuid):
  # This is in here, as opposed to the top level as recommended by the PEP
  # because then we don't have to worry about loading bottle in the unit tests
  from bottle import template

  user = User.fromUUID(user_uuid)
  # recommendation =

  renderedTemplate = template("compare.html", recommendation = json.dumps(recommendation))

  return renderedTemplate
