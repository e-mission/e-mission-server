import unittest
import sys
import os
from datetime import datetime, timedelta

from get_database import get_db, get_client_db, get_profile_db, get_uuid_db, get_pending_signup_db, get_section_db
from utils import load_database_json
sys.path.append("%s" % os.getcwd())
from dao.client import Client
from tests import common

import logging
logging.basicConfig(level=logging.DEBUG)

class TestClient(unittest.TestCase):
  def setUp(self):
    # Make sure we start with a clean slate every time
    self.serverName = 'localhost'
    common.dropAllCollections(get_db())
    logging.info("After setup, client count = %d, profile count = %d, uuid count = %d" % 
      (get_client_db().find().count(), get_profile_db().count(), get_uuid_db().count()))
    load_database_json.loadTable(self.serverName, "Stage_Modes", "tests/data/modes.json")
    
  def testInitClient(self):
    emptyClient = Client("testclient")
    self.assertEqual(emptyClient.clientName, "testclient")
    self.assertEqual(emptyClient.settings_filename, "clients/testclient/settings.json")
    self.assertEqual(emptyClient.clientJSON, None)
    self.assertEqual(emptyClient.startDatetime, None)
    self.assertEqual(emptyClient.endDatetime, None)

  def testIsEmptyClientActive(self):
    emptyClient = Client("testclient")
    self.assertFalse(emptyClient.isActive(datetime.now()))

  def updateWithTestSettings(self, client, fileName):
    client.settings_filename = fileName
    client.update(createKey = True)

  def testCreateClient(self):
    client = Client("testclient")
    client.update(createKey = False)
    self.assertEqual(client.startDatetime, datetime(2014, 10, 13))
    self.assertEqual(client.endDatetime, datetime(2014, 12, 25))
    self.assertEqual(client.isActive(datetime(2014, 11, 7)), True)
    self.assertEqual(client.getDates()[0], datetime(2014, 10, 13))
    self.assertEqual(client.getDates()[1], datetime(2014, 12, 25))

    # Reset the times in the client so that it will show as active and we will
    # get a valid set of settings    
    common.makeValid(client)
    self.assertEqual(client.isActive(datetime.now()), True)
    self.assertNotEqual(client.getSettings(), None)
    self.assertNotEqual(client.getSettings(), {})

    print client.getSettings()
    self.assertNotEqual(client.getSettings()['result_url'], None)

  def testUpdateClient(self):
    client = Client("testclient")
    self.updateWithTestSettings(client, "tests/dao/testclient/testclient_settings_update.json")

    self.assertEqual(client.startDatetime, datetime(2015, 01, 13))
    self.assertEqual(client.endDatetime, datetime(2015, 04, 25))
    self.assertEqual(client.getDates()[0], datetime(2015, 01, 13))
    self.assertEqual(client.getDates()[1], datetime(2015, 04, 25))

  def testCallMethod(self):
    client = Client("testclient")
    client.update(createKey = False)
    result = client.callMethod("classifiedCount", {'user': 'fake_user'})
    self.assertEqual(result, {'count': 0})

  def testPreRegisterNewUser(self):
    client = Client("testclient")
    client.update(createKey = False)
    common.makeValid(client)
    (resultPre, resultReg) = client.preRegister("this_is_the_super_secret_id", "fake@fake.com")
    self.assertEqual(resultPre, 1)
    self.assertEqual(resultReg, 0)

  def testPreRegisterExistingUser(self):
    from dao.user import User

    user = User.register("fake@fake.com")

    client = Client("testclient")
    client.update(createKey = False)
    common.makeValid(client)
    (resultPre, resultReg) = client.preRegister("this_is_the_super_secret_id", "fake@fake.com")
    self.assertEqual(resultPre, 0)
    self.assertEqual(resultReg, 1)

    self.assertEqual(user.getStudy(), ['testclient'])
    pendingRegs = Client.getPendingClientRegs("fake@fake.com")
    self.assertEqual(pendingRegs, [])

  def testPreRegisterFail(self):
    client = Client("testclient")
    client.update(createKey = False)
    common.makeValid(client)
    (resultPre, resultReg) = client.preRegister("this_is_the_super_secret_id", "fake@fake.com")
    self.assertEqual(resultPre, 1)
    self.assertEqual(resultReg, 0)

    pendingRegs = Client.getPendingClientRegs("fake@fake.com")
    self.assertEqual(pendingRegs, ["testclient"])

  def testPendingClientRegs(self):
    fakeEmail = "fake@fake.com"

    client = Client("testclient")
    client.update(createKey = False)
    common.makeValid(client)
    (resultPre, resultReg) = client.preRegister("this_is_the_super_secret_id", fakeEmail)
    studyList = Client.getPendingClientRegs(fakeEmail)
    self.assertEqual(studyList, ["testclient"])
   
    brokenStudyList = Client.getPendingClientRegs("bake@fake.com")
    self.assertEqual(brokenStudyList, [])

  def testPendingClientRegsDelete(self):
    fakeEmail = "fake@fake.com"

    client = Client("testclient")
    client.update(createKey = False)
    common.makeValid(client)
    (resultPre, resultReg) = client.preRegister("this_is_the_super_secret_id", fakeEmail)
    studyList = Client.getPendingClientRegs(fakeEmail)
    self.assertEqual(studyList, ["testclient"])
  
    Client.deletePendingClientRegs(fakeEmail) 
    afterDelList = Client.getPendingClientRegs(fakeEmail)
    self.assertEqual(afterDelList, [])

  def testGetSectionFilter(self):
    from dao.user import User

    fakeEmail = "fake@fake.com"

    client = Client("testclient")
    client.update(createKey = False)
    common.makeValid(client)

    (resultPre, resultReg) = client.preRegister("this_is_the_super_secret_id", fakeEmail)
    studyList = Client.getPendingClientRegs(fakeEmail)
    self.assertEqual(studyList, ["testclient"])

    user = User.register("fake@fake.com")
    self.assertEqual(user.getFirstStudy(), 'testclient')

    self.assertEqual(client.getSectionFilter(user.uuid), [])
    # Now, set the update timestamp to two weeks ago
    common.updateUserCreateTime(user.uuid)
    self.assertEqual(client.getSectionFilter(user.uuid), [{'test_auto_confirmed.prob': {'$lt': 0.9}}])

  def testClientSpecificSettersWithOverride(self):
    from dao.user import User

    fakeEmail = "fake@fake.com"

    client = Client("testclient")
    client.update(createKey = False)
    common.makeValid(client)

    (resultPre, resultReg) = client.preRegister("this_is_the_super_secret_id", fakeEmail)
    studyList = Client.getPendingClientRegs(fakeEmail)
    self.assertEqual(studyList, ["testclient"])

    user = User.register("fake@fake.com")
    self.assertEqual(user.getFirstStudy(), 'testclient')

    dummyPredModeMap = {'walking': 1.0}
    dummySection = common.createDummySection(startTime = datetime.now() - timedelta(seconds = 60 * 60),
        endTime = datetime.now(),
        startLoc = [-122, 34],
        endLoc = [-122, 35],
        predictedMode = dummyPredModeMap)

    clientSetQuery = client.clientSpecificSetters(user.uuid, dummySection['_id'], dummyPredModeMap)
    self.assertEqual(clientSetQuery, {'$set': {'test_auto_confirmed': {'mode': 1, 'prob': 1.0}}})

    # Apply the change
    get_section_db().update({'_id': dummySection['_id']}, clientSetQuery)
    retrievedSection = get_section_db().find_one({'_id': dummySection['_id']})
    self.assertEqual(retrievedSection['test_auto_confirmed']['mode'], 1)
    

  def testClientSpecificSettersNoOverride(self):
    from dao.user import User

    fakeEmail = "fake@fake.com"
    user = User.register("fake@fake.com")
    self.assertEqual(user.getFirstStudy(), None)

    dummyPredModeMap = {'walking': 1.0}
    dummySection = common.createDummySection(startTime = datetime.now() - timedelta(seconds = 60 * 60),
        endTime = datetime.now(),
        startLoc = [-122, 34],
        endLoc = [-122, 35],
        predictedMode = dummyPredModeMap)

    clientSetQuery = Client(user.getFirstStudy()).clientSpecificSetters(user.uuid, dummySection, dummyPredModeMap)
    self.assertEqual(clientSetQuery, None)

  def testClientConfirmedModeQueries(self):
    queryDict = Client.getClientConfirmedModeQueries(4)[0]
    self.assertTrue('$or' in queryDict)
    queryList = queryDict['$or']
    self.assertIn({'test_auto_confirmed.mode': 4}, queryList)

  def testClientConfirmedModeField(self):
    client = Client("testclient")
    client.update(createKey = False)
    common.makeValid(client)

    self.assertEqual(client.getClientConfirmedModeField(), 'test_auto_confirmed.mode')

  def testClientConfirmedModeQuery(self):
    client = Client("testclient")
    client.update(createKey = False)
    common.makeValid(client)

    self.assertEqual(client.getClientConfirmedModeQuery(4), {'test_auto_confirmed.mode': 4})

if __name__ == '__main__':
    unittest.main()
