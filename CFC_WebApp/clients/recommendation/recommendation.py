from uuid import UUID
import json
import ast
import sys
import os

from dao.user import User
from get_database import get_trip_db

sys.path.append("%s/../CFC_DataCollector/" % os.getcwd())
sys.path.append("%s" % os.getcwd())
from trip import E_Mission_Trip


def getResult(user_uuid):
    # This is in here, as opposed to the top level as recommended by the PEP
    # because then we don't have to worry about loading bottle in the unit tests
    from bottle import template

    user_uuid = "6433c8cf-c4c5-3741-9144-5905379ece6e"
    user = User.fromUUID(user_uuid)

    original_trip = E_Mission_Trip.trip_from_json(
        get_trip_db().find_one({'user_id': UUID(user.uuid), 'recommended_alternative': {'$exists': True}}))
    recommended_trip = originalTrip['recommended_alternative']

    del originalTrip['trip_start_datetime']
    del originalTrip['trip_end_datetime']
    del originalTrip['user_id']
    del originalTrip['pipelineFlags']

    renderedTemplate = template("clients/recommendation/result_template.html",
                                recommendedTrip=ast.literal_eval(json.dumps(recommendedtrip)),
                                originalTrip=ast.literal_eval(json.dumps(original_trip)))
    # originalSections = ast.literal_eval(json.dumps(original_sections)))

    return renderedTemplate
