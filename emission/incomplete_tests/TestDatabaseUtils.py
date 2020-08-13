from __future__ import unicode_literals
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import
from future import standard_library
standard_library.install_aliases()
from builtins import *
import unittest
from pymongo import MongoClient
from utils import load_database_json, purge_database_json, dump_database_json
from main import auth
import json

class TestDatabaseUtils(unittest.TestCase):
  def setUp(self):
    self.testUser = "test@example.com"
    self.mappedUser = ["shankari", "tamtom2000@gmail.com"]

    self.serverName = 'localhost'
    self.sampleAuthMessage1 = {'user_id': 99999999999999999, 'access_token': 'Initial_token', 'expires_in': 15551999, 'token_type': 'bearer', 'refresh_token': 'Initial_refresh_token'}
    self.sampleAuthMessage2 = {'user_id': 11111111111111111, 'access_token': 'Initial_token', 'expires_in': 15551999, 'token_type': 'bearer', 'refresh_token': 'Initial_refresh_token'}
    auth.saveAccessToken(self.sampleAuthMessage1, self.testUser)
    auth.saveAccessToken(self.sampleAuthMessage2, self.mappedUser[1])

    # We are going to try to create the groups db here, so let's remove it in
    # the setup in case there was a crash earlier or exception earlier
    Testdb = MongoClient(self.serverName).Test_database
    Groups=Testdb.Test_Groups
    Groups.remove()

  def tearDown(self):
    auth.deleteAllTokens(self.testUser)

  def testFixFormat(self):
    fixedStr = load_database_json.fixFormat("{u'distance': 66.0, u'group': u'walking', u'trackPoints': [], u'manual': False, u'steps': 131, u'startTime': u'20140407T081857-0700', u'activity': u'walking', u'duration': 90.0, u'endTime': u'20140407T082027-0700'}")
    self.assertEqual(fixedStr, '{"distance": 66.0, "group": "walking", "trackPoints": [], "manual": false, "steps": 131, "startTime": "20140407T081857-0700", "activity": "walking", "duration": 90.0, "endTime": "20140407T082027-0700"}')
    loadedJSON = json.loads(fixedStr)
    self.assertNotEqual(loadedJSON, None)

  def testLoadPurgeDatabase(self):
    Testdb = MongoClient(self.serverName).Test_database
    Sections=Testdb.Test_Sections

    purge_database_json.purgeData('localhost', self.testUser)
    self.assertEqual(Sections.count_documents({'user_id' : self.testUser}), 0)

    load_database_json.loadData(self.serverName, 'CFC_WebApp/tests/data/testLoadFile')
    self.assertEqual(Sections.count_documents({'user_id': self.testUser}), 3)

    purge_database_json.purgeData('localhost', self.testUser)
    self.assertEqual(Sections.count_documents({'user_id' : self.testUser}), 0)

  def testUserRecordPresent(self):
    userRecord = dump_database_json.getSingleUserRecord(self.testUser)
    self.assertTrue(type(userRecord) is dict)

  def testUserRecordAbsentButMapped(self):
    userRecord = dump_database_json.getSingleUserRecord(self.mappedUser[0])
    self.assertNotEqual(userRecord, None)

  def testUserRecordAbsent(self):
    userRecord = dump_database_json.getSingleUserRecord("Fake")
    self.assertEqual(userRecord, None)

  def testDumpSections(self):
    load_database_json.loadData(self.serverName, 'CFC_WebApp/tests/data/testLoadFile')
    dump_database_json.dumpData('localhost', '/tmp/testDumpFile')
    dumpedStr = open('/tmp/testDumpFile').readline()
    expectedStr = open('CFC_WebApp/tests/data/expectedDumpFile').readline()
    self.assertEqual(len(dumpedStr), len(expectedStr))
    self.assertEqual(dumpedStr, expectedStr)

  def testLoadTable(self):
    load_database_json.loadTable(self.serverName, "Test_Groups", "CFC_DataCollector/tests/data/groups.json")
    Testdb = MongoClient(self.serverName).Test_database
    GroupsColl=Testdb.Test_Groups
    self.assertEqual(GroupsColl.estimated_document_count(), 5)
    GroupsColl.remove()
    self.assertEqual(GroupsColl.estimated_document_count(), 0)

  def testDumpTable(self):
    rawFile = "CFC_DataCollector/tests/data/groups.json"
    dumpedFile = "/tmp/dumpedGroups"

    load_database_json.loadTable(self.serverName, "Test_Groups", rawFile)
    dump_database_json.dumpTable(self.serverName, "Test_Groups", dumpedFile)

    expectedStr = open(rawFile).readline()
    dumpedStr = open(dumpedFile).readline() + "\n"

    self.assertEqual(len(dumpedStr), len(expectedStr))
    self.assertEqual(dumpedStr, expectedStr)

if __name__ == '__main__':
    unittest.main()
