from dao.user import User
import json 

def getCommonTrips(user):
    """
    Gets common trips for a given user
    """
    #TODO: Don't just get a random trip
    

def getResult(user_uuid):
  # This is in here, as opposed to the top level as recommended by the PEP
  # because then we don't have to worry about loading bottle in the unit tests
  from bottle import template
  renderedTemplate = template("clients/commontrips/result_template.html")
  return renderedTemplate
