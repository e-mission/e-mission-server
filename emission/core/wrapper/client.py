from __future__ import unicode_literals
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import
#
# In the current iteration, there is a client object that can be loaded from
# the filesystem into the database and its settings loaded from the database.
# There are no special settings (e.g. active/inactive).
#
# I have no idea how this will be used, but it is nice^H^H^H^H, unit tested code,
# so let us keep it around a bit longer
#
# Ah but this assumes that the settings file is in `emission/clients/` and we
# just deleted that entire directory. Changing this to conf for now...

from future import standard_library
standard_library.install_aliases()
from builtins import str
from builtins import *
from builtins import object
import json
import logging
import dateutil.parser
from datetime import datetime

# Our imports
from emission.core.get_database import get_profile_db, get_client_db

class Client(object):
  def __init__(self, clientName):
    # TODO: write background process to ensure that there is only one client with each name
    # Maybe clean up unused clients?
    self.clientName = clientName
    self.settings_filename = "conf/clients/%s.settings.json" % self.clientName
    self.__reload()

  # Smart settings call, which returns the override settings if the client is
  # active, and 
  def getSettings(self):
      logging.debug("For client %s, returning settings %s" % (self.clientName, self.clientJSON['client_settings']))
      return self.clientJSON['client_settings']

  def __reload(self):
    self.clientJSON = None
    if self.clientName is not None:
      self.clientJSON = get_client_db().find_one({'name': self.clientName})

  # Figure out if the JSON object here should always be passed in
  # Having it be passed in is a lot more flexible
  # Let's compromise for now by passing it in and seeing how much of a hassle it is
  # That will also ensure that the update_client script is not a complete NOP
  def __update(self, newEntry):
    get_client_db().update({'name': self.clientName}, newEntry, upsert = True)
    self.__reload()

  def update(self, createKey = True):
    import uuid 
    with open(self.settings_filename) as fp:
        newEntry = json.load(fp)
    if createKey:
      newEntry['key'] = str(uuid.uuid4())
    # logging.info("Updating with new entry %s" % newEntry)
    self.__update(newEntry)
    return newEntry['key']

  def getClientKey(self):
    if self.clientJSON is None:
        return None
    logging.debug("About to return %s from JSON %s" % (self.clientJSON['key'], self.clientJSON))
    return self.clientJSON['key']

  def clientSpecificSetters(self, uuid, sectionId, predictedModeMap):
      return None
