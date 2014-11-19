# Functions that manipulate both user and client information. It seemed better
# to call that out into a separate module, rather than decide whether to stick
# it into User or into Client.
from dao.user import User
from dao.client import Client

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
