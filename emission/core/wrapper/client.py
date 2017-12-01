import json
import logging
import dateutil.parser
from datetime import datetime

# Our imports
from emission.core.get_database import get_profile_db, get_client_db, get_pending_signup_db

class Client:
  def __init__(self, clientName):
    # TODO: write background process to ensure that there is only one client with each name
    # Maybe clean up unused clients?
    self.clientName = clientName
    self.settings_filename = "emission/clients/%s/settings.json" % self.clientName
    self.__reload()

  # Smart settings call, which returns the override settings if the client is
  # active, and 
  def getSettings(self):
    if (self.isActive(datetime.now())):
      logging.debug("For client %s, returning settings %s" % (self.clientName, self.clientJSON['client_settings']))
      return self.clientJSON['client_settings']
    else:
      # Returning empty dict instead of None to make the client code, which
      # will want to merge this, easier
      logging.debug("For client %s, active = false, returning {}" % (self.clientName))
      return {}

  # Figure out if the JSON object here should always be passed in
  # Having it be passed in is a lot more flexible
  # Let's compromise for now by passing it in and seeing how much of a hassle it is
  # That will also ensure that the update_client script is not a complete NOP
  def __update(self, newEntry):
    get_client_db().update({'name': self.clientName}, newEntry, upsert = True)
    self.__reload()

  def update(self, createKey = True):
    import uuid 
    newEntry = json.load(open(self.settings_filename))
    if createKey:
      newEntry['key'] = str(uuid.uuid4())
    # logging.info("Updating with new entry %s" % newEntry)
    self.__update(newEntry)
    return newEntry['key']

  def __loadModule(self):
    import importlib
    clientModule = importlib.import_module("emission.clients.%s.%s" % (self.clientName, self.clientName))
    return clientModule

  def callMethod(self, methodName, request):
    clientModule = self.__loadModule()
    logging.debug("called client with %s %s" % (self.clientName, methodName))
    # import clients.carshare.carshare as clientModule
    method = getattr(clientModule, methodName)
    logging.debug("Invoking %s on module %s" % (method, clientModule))
    return method(request)

  def getClientKey(self):
    if self.clientJSON is None:
        return None
    logging.debug("About to return %s from JSON %s" % (self.clientJSON['key'], self.clientJSON))
    return self.clientJSON['key']

  def clientSpecificSetters(self, uuid, sectionId, predictedModeMap):
    if self.isActive(datetime.now()):
      return self.__loadModule().clientSpecificSetters(uuid, sectionId, predictedModeMap)
    else:
      return None

