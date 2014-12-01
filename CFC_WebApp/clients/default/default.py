import logging
from main import carbon
from dao.user import User
import json

def getResult(user_uuid):
  # This is in here, as opposed to the top level as recommended by the PEP
  # because then we don't have to worry about loading bottle in the unit tests
  from bottle import template

  user = User.fromUUID(user_uuid)
  (ignore, currFootprint) = user.getScore()

  if currFootprint == 0:
    currFootprint = carbon.getFootprintCompare(user_uuid)
    user.setScores(None, currFootprint)

  (myModeShareCount, avgModeShareCount,
     myModeShareDistance, avgModeShareDistance,
     myModeCarbonFootprint, avgModeCarbonFootprint,
     myModeCarbonFootprintNoLongMotorized, avgModeCarbonFootprintNoLongMotorized, # ignored
     myOptimalCarbonFootprint, avgOptimalCarbonFootprint,
     myOptimalCarbonFootprintNoLongMotorized, avgOptimalCarbonFootprintNoLongMotorized) = currFootprint

  renderedTemplate = template("compare.html",
                      myModeShareCount = json.dumps(myModeShareCount),
                      avgModeShareCount = json.dumps(avgModeShareCount),
                      myModeShareDistance = json.dumps(myModeShareDistance),
                      avgModeShareDistance = json.dumps(avgModeShareDistance),
                      myModeCarbonFootprint = json.dumps(myModeCarbonFootprint),
                      avgModeCarbonFootprint = json.dumps(avgModeCarbonFootprint),
                      myOptimalCarbonFootprint = json.dumps(myOptimalCarbonFootprint),
                      avgOptimalCarbonFootprint = json.dumps(avgOptimalCarbonFootprint))
                  
  # logging.debug(renderedTemplate)
  return renderedTemplate
