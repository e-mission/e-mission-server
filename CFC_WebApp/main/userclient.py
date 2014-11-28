# Functions that manipulate both user and client information. It seemed better
# to call that out into a separate module, rather than decide whether to stick
# it into User or into Client.
from dao.user import User
from dao.client import Client
from get_database import get_profile_db

def getUserClient(user_uuid):
    study = User.fromUUID(user_uuid).getFirstStudy()
    if study != None:
      client = Client(study)
      return client
    else:
      # User is not part of any study, so no additional filtering is needed
      return None

def getClientSpecificQueryFilter(user_uuid):
    client = getUserClient(user_uuid)
    if client == None:
      return []
    else:
      return client.getSectionFilter(user_uuid)

def getClientSpecificResult(user_uuid):
    client = getUserClient(user_uuid)
    if client == None:
      return None
    else:
      return client.getResult(user_uuid)

def runClientSpecificBackgroundTasks(user_uuid, defaultTasks):
    client = getUserClient(user_uuid)
    if client == None:
      defaultTasks(user_uuid)
    else:
      client.runBackgroundTasks(user_uuid)

def getClientQuery(clientName):
    if clientName is None:
        return {'study_list': {'$size': 0}}
    else:
        return {'study_list': {'$in': [clientName]}}

def countForStudy(study):
  return get_profile_db().find(getClientQuery(study)).count()

def getUsersForClient(clientName):
  # Find all users for this client
  client_uuids = []
  for user in get_profile_db().find(getClientQuery(clientName)):
    client_uuids.append(user)
