from __future__ import print_function
from __future__ import unicode_literals
from __future__ import division
from __future__ import absolute_import
# Standard imports
from future import standard_library
standard_library.install_aliases()
from builtins import *
import unittest
import sys
import os
from datetime import datetime, timedelta
import logging

# Our imports
import emission.core.get_database as edb
from emission.core.get_database import get_client_db, get_profile_db, get_uuid_db, get_section_db
from emission.core.wrapper.client import Client
from emission.tests import common
from emission.core.wrapper.user import User

import emission.tests.common as etc

class TestClient(unittest.TestCase):
  def setUp(self):
    # Make sure we start with a clean slate every time
    self.serverName = 'localhost'
    common.dropAllCollections(edb._get_current_db())

    import shutil
    self.config_path = "conf/clients/testclient.settings.json"
    shutil.copyfile("%s.sample" % self.config_path,
                    self.config_path)

    logging.info("After setup, client count = %d, profile count = %d, uuid count = %d" % 
      (get_client_db().estimated_document_count(), get_profile_db().estimated_document_count(), get_uuid_db().estimated_document_count()))
    common.loadTable(self.serverName, "Stage_Modes", "emission/tests/data/modes.json")

  def tearDown(self):
    import os
    os.remove(self.config_path)
    
  def testInitClient(self):
    emptyClient = Client("testclient")
    self.assertEqual(emptyClient.clientName, "testclient")
    self.assertEqual(emptyClient.settings_filename, self.config_path)
    self.assertEqual(emptyClient.clientJSON, None)

  def updateWithTestSettings(self, client, fileName):
    client.settings_filename = fileName
    client.update(createKey = True)

  def testCreateClient(self):
    client = Client("testclient")
    client.update(createKey = False)

    # Reset the times in the client so that it will show as active and we will
    # get a valid set of settings    
    common.makeValid(client)
    self.assertNotEqual(client.getSettings(), None)
    self.assertNotEqual(client.getSettings(), {})

    print(client.getSettings())
    self.assertNotEqual(client.getSettings()['result_url'], None)

  def testUpdateClient(self):
    client = Client("testclient")
    self.updateWithTestSettings(client, "emission/tests/coreTests/wrapperTests/testclient/testclient_settings_update.json")

if __name__ == '__main__':
    etc.configLogging()
    unittest.main()
