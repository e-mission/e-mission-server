import json
import logging

from get_database import get_profile_db, get_client_db, get_pending_signup_db
import dateutil.parser
from datetime import datetime

import clients.common

class Client:
  def __init__(self, clientName):
    # TODO: write background process to ensure that there is only one client with each name
    # Maybe clean up unused clients?
    self.clientName = clientName
    self.settings_filename = "clients/%s/settings.json" % self.clientName
    self.__reload()

  def __reload(self):
    self.clientJSON = None
    if self.clientName is not None:
      self.clientJSON = get_client_db().find_one({'name': self.clientName})

    # clientJSON can be None if we are creating an entry for the first time
    if self.clientJSON is None:
      # Avoid Attribute error while trying to determine whether the client is active
      self.startDatetime = None
      self.endDatetime = None
    else:
      # Do eagerly or lazily? Can also do super lazily and have 
      self.startDatetime = dateutil.parser.parse(self.clientJSON['start_date'])
      self.endDatetime = dateutil.parser.parse(self.clientJSON['end_date'])

  def isActive(self, now):
    logging.debug("Comparing %s to %s and %s" % (now, self.startDatetime, self.endDatetime))
    if self.startDatetime is None:
      return False
    else:
      if  self.startDatetime > now:
        # Study has not yet started
        return False
      else:
        if self.endDatetime is None:
          # Study has no end time
          return True
        else:
          if self.endDatetime > now:
            # study has not yet ended
            return True
          else:
            # study has already ended
            return False
   
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

  def getDates(self):
    return (self.startDatetime, self.endDatetime)

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
    clientModule = importlib.import_module("clients.%s.%s" % (self.clientName, self.clientName))
    return clientModule

  def callMethod(self, methodName, request):
    clientModule = self.__loadModule()
    logging.debug("called client with %s %s" % (self.clientName, methodName))
    # import clients.carshare.carshare as clientModule
    method = getattr(clientModule, methodName)
    logging.debug("Invoking %s on module %s" % (method, clientModule))
    return method(request)

  def getClientKey(self):
    logging.debug("About to return %s from JSON %s" % (self.clientJSON['key'], self.clientJSON))
    return self.clientJSON['key']

  def __validateKey(self, clientKey):
    if (not self.isActive(datetime.now())):
      logging.info("Client %s is not yet active, so key %s is not valid" %
        (self.clientName, clientKey))
      return False
    client_key = self.getClientKey()
    if client_key == clientKey:
      return True
    else:
      logging.info("For client %s, incoming key %s does not match stored key %s!" %
        (self.clientName, clientKey, client_key))
      return False

# What should we do if a user registers again after they have installed the app?
# Options are:
# - NOP
# - update the study field
# - Return error
# For now, we update the study field, pending discussions with Maita on error reporting
# What should we do if a user registers for a study after having installed
# the app or having participated in a different study?
# - add the study to the list of registered studies (but we don't support multiple studies!)
# - update the registered study
# - return an error
# For now, we update the registered study since it makes life easiest for us
# TODO: Figure out what to do here
# Also, note that always inserting it is also fine if we move to an eventual
# consistency model, since we will eventually clean it up again. The end
# result will still be a NOP, though
  def __preRegister(self, userEmail):
    from dao.user import User
    from main import userclient

    if User.isRegistered(userEmail):
      User.fromEmail(userEmail).setStudy(self.clientName)
    else:
      pendingDoc = {
        'user_email': userEmail,
        'study': self.clientName,
        'last_update': datetime.now()}
      # Should I do insert or upsert here? If a user has pre-registered for one
      # study and then pre-registers for another study before registering, do we
      # want to throw an error or just update silently?
      # Update silently for now
      writeResult = get_pending_signup_db().update({'user_email': userEmail}, pendingDoc, upsert=True)
      print 'in __preRegister, writeResult = %s' % writeResult
      if 'err' in writeResult and writeResult['err'] is not None:
        e = Exception()
        e.code = writeResult['err'][0]["code"]
        e.msg = writeResult['err'][0]["errmsg"]
        raise e
    return (get_pending_signup_db().find({'study': self.clientName}).count(),
            userclient.countForStudy(self.clientName))

  def preRegister(self, clientKey, userEmail):
    if not self.__validateKey(clientKey):
      e = Exception()
      e.code = 403
      e.msg = "This is not the client key for your study, or your study has already ended. Please contact e-mission@lists.eecs.berkeley.edu to obtain a client key, or restart your study"
      raise e
    return self.__preRegister(userEmail)

  def __callJavascriptCallback(self, methodName, params):
    if self.isActive(datetime.now()):
      clientModule = self.__loadModule()
      method = getattr(clientModule, methodName)
      return method(params)
    else:
      return None

  def callJavascriptCallback(self, clientKey, method, request):
    if not self.__validateKey(clientKey):
      e = Exception()
      e.code = 403
      e.msg = "This is not the client key for your study, or your study has already ended. Please contact e-mission@lists.eecs.berkeley.edu to obtain a client key, or restart your study"
      raise e
    return self.__callJavascriptCallback(method, request)

  # BEGIN: Standard customization hooks
  def getClientConfirmedModeQuery(self, mode):
    if self.isActive(datetime.now()):
      clientModeField = self.getClientConfirmedModeField()
      return {clientModeField: mode}
    else:
      return {}

  def getClientConfirmedModeField(self):
    if self.isActive(datetime.now()):
      clientModule = self.__loadModule()
      return clientModule.getClientConfirmedModeField()
    else:
      return None

  def getSectionFilter(self, uuid):
    if self.isActive(datetime.now()):
      return self.__loadModule().getSectionFilter(uuid)
    else:
      return []

  def getResult(self, uuid):
    if self.isActive(datetime.now()):
        return self.__loadModule().getResult(uuid)
    else:
        return None

  def clientSpecificSetters(self, uuid, sectionId, predictedModeMap):
    if self.isActive(datetime.now()):
      return self.__loadModule().clientSpecificSetters(uuid, sectionId, predictedModeMap)
    else:
      return None

  def runBackgroundTasks(self, uuid):
    if self.isActive(datetime.now()):
        self.__loadModule().runBackgroundTasks(uuid)
    else:
        logging.debug("Client is not active, skipping call...")
  # END: Standard customization hooks

  # This reads the combined set of queries from all clients
  # Read the design decisions for an example of how to improve this
  @staticmethod
  def getClientConfirmedModeQueries(mode):
    queryList = clients.common.getConfirmFields()
    queryListWithMode = [{query: mode} for query in queryList]
    return [{'$or': queryListWithMode}]

  @staticmethod
  def getPendingClientRegs(userName):
    studyList = []
    userEmailQuery = {'user_email': userName}
    pendingReg = get_pending_signup_db().find_one(userEmailQuery)
    if pendingReg != None:
      studyList = [pendingReg['study']]
    return studyList

  @staticmethod
  def deletePendingClientRegs(userName):
    userEmailQuery = {'user_email': userName}
    get_pending_signup_db().remove(userEmailQuery)
