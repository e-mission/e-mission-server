import logging
import unittest
import json
import sys
import os
import mock
from utils import load_database_json

sys.path.append("%s" % os.getcwd())
import get_database
from main import common
from uuid import UUID
from datetime import datetime, timedelta

logging.basicConfig(level=logging.DEBUG)

class TestCommon(unittest.TestCase):
  def setUp(self):
    import tests.common

    self.serverName = 'localhost'

    # Make sure we start with a clean slate every time
    tests.common.dropAllCollections(get_database.get_db())
    
    # Load modes, otherwise the queries won't work properly
    load_database_json.loadTable(self.serverName, "Stage_Modes", "tests/data/modes.json")
    self.dayago = datetime.now() - timedelta(days=1)
    self.now = datetime.now()

  def testAddModeIdNonAndSpec(self):
    inSpec = {'type': 'move'}
    retSpec = common.addModeIdToSpec(inSpec, 3)
    self.assertEqual(retSpec["$and"][0], {"type" : "move"})
    self.assertIn("$or", retSpec['$and'][1])
    self.assertEqual(retSpec['$and'][1]["$or"][0], {"corrected_mode": 3})
    self.assertEqual(retSpec['$and'][1]["$or"][1]['$and'][1], {"confirmed_mode": 3})
    self.assertIn({'test_auto_confirmed.mode': 3}, retSpec['$and'][1]["$or"][2]['$and'][2]["$or"])

  def testAddModeIdNone(self):
    inSpec = None
    retSpec = common.addModeIdToSpec(inSpec, 3)
    self.assertEquals(retSpec['$or'][0], {'corrected_mode': 3})
    self.assertEquals(retSpec['$or'][1]['$and'][0], {'corrected_mode': {'$exists': False}})
    self.assertEquals(retSpec['$or'][2]['$and'][0], {'corrected_mode': {'$exists': False}})
    self.assertEquals(retSpec['$or'][2]['$and'][1], {'confirmed_mode': {'$exists': False}})
    self.assertIn({'test_auto_confirmed.mode': 3}, retSpec['$or'][2]['$and'][2]['$or'])

  def testAddModeIdAndSpec(self):
    inSpec = {"$and": [{'type': 'move'}, {'commute':'on'}]}
    retSpec = common.addModeIdToSpec(inSpec, 3)
    logging
    self.assertIn({'type': 'move'}, retSpec['$and'])
    self.assertIn({'commute': 'on'}, retSpec['$and'])
    self.assertIn({'corrected_mode': 3}, retSpec['$and'][2]['$or'])
    self.assertIn({'confirmed_mode': 3}, retSpec['$and'][2]['$or'][1]['$and'])
    self.assertIn({'test_auto_confirmed.mode': 3}, retSpec['$and'][2]['$or'][2]['$and'][2]['$or'])

  def testAddModeIdAndSpecTwice(self):
    inSpec = {"$and": [{'type': 'move'}, {'commute':'on'}]}
    expectedSpec = {"$and": [{'type': 'move'}, {'commute':'on'}, {'confirmed_mode': 4}]}
    firstSpec = common.addModeIdToSpec(inSpec, 3)

    retSpec = common.addModeIdToSpec(inSpec, 4)
    logging.debug(retSpec)
    self.assertIn({'type': 'move'}, retSpec['$and'])
    self.assertIn({'commute': 'on'}, retSpec['$and'])
    self.assertIn({'corrected_mode': 4}, retSpec['$and'][2]['$or'])
    self.assertIn({'confirmed_mode': 4}, retSpec['$and'][2]['$or'][1]['$and'])
    self.assertIn({'test_auto_confirmed.mode': 4}, retSpec['$and'][2]['$or'][2]['$and'][2]['$or'])

  def setupClientTest(self):
    # At this point, the more important test is to execute the query and see
    # how well it works
    from dao.user import User
    from dao.client import Client
    import tests.common

    fakeEmail = "fake@fake.com"

    client = Client("testclient")
    client.update(createKey = False)
    tests.common.makeValid(client)

    (resultPre, resultReg) = client.preRegister("this_is_the_super_secret_id", fakeEmail)
    studyList = Client.getPendingClientRegs(fakeEmail)
    self.assertEqual(studyList, ["testclient"])

    user = User.register("fake@fake.com")
    self.assertEqual(user.getFirstStudy(), 'testclient')

    dummyPredModeMap = {'walking': 1.0}
    dummySection = tests.common.createDummySection(
        startTime = datetime.now() - timedelta(seconds = 60 * 60),
        endTime = datetime.now(),
        startLoc = [-122, 34],
        endLoc = [-122, 35],
        predictedMode = dummyPredModeMap)
    return (user, dummySection, dummyPredModeMap)


  def testConfirmationModeQueryAutoNoManual(self):
    from dao.client import Client

    (user, dummySection, dummyPredModeMap) = self.setupClientTest()
    clientSetQuery = Client(user.getFirstStudy()).clientSpecificSetters(user.uuid, dummySection, dummyPredModeMap)

    # Apply the change
    get_database.get_section_db().update({'_id': dummySection['_id']}, clientSetQuery)
    retrievedSection = get_database.get_section_db().find_one({'_id': dummySection['_id']})
    self.assertEqual(retrievedSection['test_auto_confirmed']['mode'], 1)

    retrieveByQuery = get_database.get_section_db().find(common.getConfirmationModeQuery(1))
    for entry in retrieveByQuery:
      print entry
    self.assertEqual(retrieveByQuery.count(), 1)

  def testConfirmationModeQueryManualAndAuto(self):
    from dao.client import Client

    (user, dummySection, dummyPredModeMap) = self.setupClientTest()
    clientSetQuery = Client(user.getFirstStudy()).clientSpecificSetters(user.uuid, dummySection, dummyPredModeMap)

    # Apply the change
    get_database.get_section_db().update({'_id': dummySection['_id']}, clientSetQuery)
    retrievedSection = get_database.get_section_db().find_one({'_id': dummySection['_id']})
    self.assertEqual(retrievedSection['test_auto_confirmed']['mode'], 1)

    get_database.get_section_db().update({'_id': dummySection['_id']}, {'$set': {'confirmed_mode': 4}})

    retrieveByQuery = get_database.get_section_db().find(common.getConfirmationModeQuery(1))
    for entry in retrieveByQuery:
      print entry
    self.assertEqual(retrieveByQuery.count(), 0)

    retrieveByQuery = get_database.get_section_db().find(common.getConfirmationModeQuery(4))
    for entry in retrieveByQuery:
      print entry
    self.assertEqual(retrieveByQuery.count(), 1)

  def testConfirmationModeQueryCorrectedManualAndAuto(self):
    from dao.client import Client

    (user, dummySection, dummyPredModeMap) = self.setupClientTest()
    clientSetQuery = Client(user.getFirstStudy()).clientSpecificSetters(user.uuid, dummySection, dummyPredModeMap)

    # Apply the change
    get_database.get_section_db().update({'_id': dummySection['_id']}, clientSetQuery)
    retrievedSection = get_database.get_section_db().find_one({'_id': dummySection['_id']})
    self.assertEqual(retrievedSection['test_auto_confirmed']['mode'], 1)

    get_database.get_section_db().update({'_id': dummySection['_id']}, {'$set': {'confirmed_mode': 4}})
    get_database.get_section_db().update({'_id': dummySection['_id']}, {'$set': {'corrected_mode': 9}})

    retrieveByQuery = get_database.get_section_db().find(common.getConfirmationModeQuery(1))
    for entry in retrieveByQuery:
      print entry
    self.assertEqual(retrieveByQuery.count(), 0)

    retrieveByQuery = get_database.get_section_db().find(common.getConfirmationModeQuery(4))
    for entry in retrieveByQuery:
      print entry
    self.assertEqual(retrieveByQuery.count(), 0)

    retrieveByQuery = get_database.get_section_db().find(common.getConfirmationModeQuery(9))
    for entry in retrieveByQuery:
      print entry
    self.assertEqual(retrieveByQuery.count(), 1)

  def testConfirmationModeQueryManualNotAuto(self):
    from dao.client import Client

    (user, dummySection, dummyPredModeMap) = self.setupClientTest()
    get_database.get_section_db().update({'_id': dummySection['_id']}, {'$set': {'confirmed_mode': 4}})

    retrieveByQuery = get_database.get_section_db().find(common.getConfirmationModeQuery(1))
    self.assertEqual(retrieveByQuery.count(), 0)

    retrieveByQuery = get_database.get_section_db().find(common.getConfirmationModeQuery(4))
    self.assertEqual(retrieveByQuery.count(), 1)

  def testConfirmationModeQueryNeither(self):
    from dao.client import Client

    (user, dummySection, dummyPredModeMap) = self.setupClientTest()
    retrieveByQuery = get_database.get_section_db().find(common.getConfirmationModeQuery(1))
    self.assertEqual(retrieveByQuery.count(), 0)

    retrieveByQuery = get_database.get_section_db().find(common.getConfirmationModeQuery(4))
    self.assertEqual(retrieveByQuery.count(), 0)

  @staticmethod
  def getDummySection(predictedMode, confirmedMode):
    import tests.common

    return tests.common.createDummySection(
            datetime.now()+timedelta(hours = -1),
            datetime.now(),
            [1, -1], [-1, 1], predictedMode, confirmedMode)

  def testGetClassifiedRatioNoTrips(self):

    self.assertEqual(common.getClassifiedRatio('this is fake', self.dayago, self.now), 0)

  def testGetClassifiedRatioWithoutPredictions(self):
    from copy import copy

    # This doesn't really require track points, so let us insert fake data that 
    # has distances, predicted modes and confirmed modes
    # 3 sections are confirmed, 3 sections are not yet predicted, classifiedRatio = 1.0
    (user, dummySection, dummyPredModeMap) = self.setupClientTest()

    for i in range(0, 3):
        predSection = copy(dummySection)
        predSection['_id'] = "%s confirmed" % (i)
        predSection['type'] = 'move'
        predSection['confirmed_mode'] = "5"
        predSection['user_id'] = user.uuid
        get_database.get_section_db().insert(predSection)

        noPredSection = copy(dummySection)
        noPredSection['_id'] = "%s nopred" % (i)
        noPredSection['type'] = 'move'
        noPredSection['user_id'] = user.uuid
        del noPredSection['predicted_mode']
        self.assertIn('predicted_mode', predSection)
        get_database.get_section_db().insert(noPredSection)

    logging.debug("After inserting sections, count is %s" % get_database.get_section_db().find().count())
    logging.debug("Manual query count = %s" % get_database.get_section_db().find({'$and': [{'source': 'Shankari'}, {'user_id': user.uuid}, {'predicted_mode': {'$exists': True}}, {'type': 'move'}]}).count())
    self.assertEqual(common.getClassifiedRatio(user.uuid, self.dayago, self.now), 1)

  def testGetClassifiedRatioWithPredictions(self):
    from copy import copy

    # This doesn't really require track points, so let us insert fake data that 
    # has distances, predicted modes and confirmed modes
    # 3 sections are confirmed, 3 sections are not yet predicted, classifiedRatio = 1.0
    (user, dummySection, dummyPredModeMap) = self.setupClientTest()

    for i in range(0, 3):
        predSection = copy(dummySection)
        predSection['_id'] = "%s confirmed" % (i)
        predSection['type'] = 'move'
        predSection['confirmed_mode'] = "5"
        predSection['user_id'] = user.uuid
        get_database.get_section_db().insert(predSection)

        noPredSection = copy(dummySection)
        noPredSection['_id'] = "%s nopred" % (i)
        noPredSection['type'] = 'move'
        noPredSection['user_id'] = user.uuid
        # moves collect currently sets the confirmed_mode to "", so we must set it here too
        # Otherwise the query won't work
        noPredSection['confirmed_mode'] = ""

        self.assertIn('predicted_mode', predSection)
        self.assertIn('predicted_mode', noPredSection)
        self.assertIn('confirmed_mode', predSection)
        get_database.get_section_db().insert(noPredSection)

    logging.debug("After inserting sections, count is %s" % get_database.get_section_db().find().count())
    logging.debug("Manual query count = %s" % get_database.get_section_db().find({'$and': [{'source': 'Shankari'}, {'user_id': user.uuid}, {'predicted_mode': {'$exists': True}}, {'type': 'move'}]}).count())
    logging.debug("Manual query count classified = %s" % get_database.get_section_db().find({'$and': [{'source': 'Shankari'}, {'user_id': user.uuid}, {'predicted_mode': {'$exists': True}}, {'type': 'move'}, {'confirmed_mode': {"$ne": ''}} ]}).count())

    self.assertEqual(common.getClassifiedRatio(user.uuid, self.dayago, self.now), 3.0/6)
    two_days_ago = datetime.now() - timedelta(days=2)
    self.assertEqual(common.getClassifiedRatio(user.uuid, two_days_ago, self.dayago), 0)

  def testBSONExceptionHandling(self):
    (user, dummySection, dummyPredModeMap) = self.setupClientTest()
    # This mock test only works if the access to the mocked function is in
    # here, not in common. I found this entry on stackoverflow on mocking
    # module methods, but trying the appropriate sequence in the REPL still
    # doesn't work. I think that this might be because of the multiple copies
    # of get_database, so that the get_database loaded from common is not the
    # same as the get_database loaded here.
    # So I will leave this half-baked test in here, and will return to it after
    # fixing the mess around get_database loading
    with mock.patch('get_database.get_section_db', side_effect=Exception("bson.errors.InvalidDocument")) as func1:
        try:
            get_database.get_section_db()
        except:
            print "Exception thrown!"
        # although the database was called
        self.assertEquals(func1.called, True)
        func1.resetMock()

if __name__ == '__main__':
    unittest.main()
