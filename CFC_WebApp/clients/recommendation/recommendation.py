import logging
from main import carbon, stats
from dao.user import User
import time as systime
from datetime import datetime, time, timedelta
from get_database import get_trip_db, get_section_db, get_alternatives_db
import json

# BEGIN: Code to get and set client specific fields in the profile (currentScore and previousScore)
# END: Code to get and set client specific fields in the profile (currentScore and previousScore)

def getResult(user_uuid):
  # This is in here, as opposed to the top level as recommended by the PEP
  # because then we don't have to worry about loading bottle in the unit tests
  from bottle import template

  user = User.fromUUID(user_uuid)

  recommendedTrip = get_alternatives_db.find_one({ 'user_id': user.uuid], 'recommended': True })
  originalTrip = get_trip_db().find_one({ 'trip_id': recommendedTrip['trip_id'] })
  originalSections = list(get_section_db().find({ 'trip_id': originalTrip['trip_id'] }))

  renderedTemplate = template("clients/recommendation/result_template.html",
                              recommendedTrip = recommendedTrip,
                              originalTrip = originalTrip,
                              originalSections = originalSections)

  return renderedTemplate
