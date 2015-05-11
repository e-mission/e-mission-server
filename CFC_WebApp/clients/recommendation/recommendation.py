import logging
from main import carbon, stats
from dao.user import User
import time as systime
from datetime import datetime, time, timedelta
from get_database import get_trip_db, get_section_db
from uuid import UUID
import json
import ast

def getResult(user_uuid):
  # This is in here, as opposed to the top level as recommended by the PEP
  # because then we don't have to worry about loading bottle in the unit tests
  from bottle import template

  original_trip = get_trip_db().find_one({'user_id': user_uuid, 'recommended_alternative': {'$exists': True}})

  if original_trip is None:
      return template("clients/recommendation/no_recommendation.html")

  del original_trip['trip_start_datetime']
  del original_trip['trip_end_datetime']
  del original_trip['user_id']
  del original_trip['pipelineFlags']
  del original_trip['recommended_alternative']['user_id']

  recommended_trip = original_trip['recommended_alternative']

  original_sections = list(get_section_db().find({'trip_id': original_trip['trip_id']}))
  for section in original_sections:
    del section['user_id']
    del section['section_start_datetime']
    del section['section_end_datetime']
    del section['retained']
    del section['manual']
    del section['commute']

  renderedTemplate = template("clients/recommendation/result_template.html",
                              originalSections = ast.literal_eval(json.dumps(original_sections)),
                              recommendedTrip = ast.literal_eval(json.dumps(recommended_trip)))

  return renderedTemplate
