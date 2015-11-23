# Standard imports
import unittest
import logging
import json
import time

# Our imports
from emission.core.get_database import get_client_stats_db, get_server_stats_db, get_result_stats_db
from emission.net.int_service.giles import archiver
from emission.net.api import stats

logging.basicConfig(level=logging.DEBUG)

class TestArchiver(unittest.TestCase):
  def setUp(self):
    self.path = '/archiver_test_path'
    self.archiver = archiver.StatArchiver(self.path)

  def tearDown(self):
    self.archiver.remove()

  # @TODO: Rewrite this test, it doesn't really test much
  def testInsertEntryWithMetadata(self):
    entry = {
        'user': '3a307244-ecf1-3e6e-a9a7-3aaf101b40fa',
        'reading': 0.189722061157,
        'ts': 1417725167,
        'stat': 'POST /tripManager/getUnclassifiedSections',
        'metadata': {'key': 'val'}
    }
    result = self.archiver.insert(entry)
    self.assertNotEqual(result, None)

  # @TODO: Rewrite this test, it doesn't really test much
  def testInsertEntryWithoutMetadata(self):
    entry = {
        'user': '3a307244-ecf1-3e6e-a9a7-3aaf101b40fa',
        'reading': 0.189722061157,
        'ts': 1417725167,
        'stat': 'POST /tripManager/getUnclassifiedSections'
    }
    result = self.archiver.insert(entry)
    self.assertNotEqual(result, None)

  def testQueryTags(self):
    entry = {
        'user': '3a307244-ecf1-3e6e-a9a7-3aaf101b40fa',
        'reading': 0.189722061157,
        'ts': 1417725167,
        'stat': 'POST /tripManager/getUnclassifiedSections',
        'metakey': 'metaval'
    }

    self.archiver.insert(entry)
    savedEntries = self.archiver.query_tags()
    self.assertEquals(len(savedEntries), 1)
    entry = savedEntries[0]
    self.assertEquals(entry['Path'], self.path)
    self.assertEquals(entry['Metadata']['user'], '3a307244-ecf1-3e6e-a9a7-3aaf101b40fa')
    self.assertEquals(entry['Metadata']['stat'], 'POST /tripManager/getUnclassifiedSections')
    self.assertEquals(entry['Metadata']['metakey'], 'metaval')

  def testQueryReadings(self):
    entry1 = {
        'user': '3a307244-ecf1-3e6e-a9a7-3aaf101b40fa',
        'reading': 0.189722061157,
        'ts': 1417725167,
        'stat': 'POST /tripManager/fakeendpoint1',
        'metadata': {'key': 'val'}
    }

    entry2 = {
        'user': 'abcdefgh-ecf1-3e6e-a9a7-3aaf101b40fa',
        'reading': 0.36,
        'ts': 1417725167,
        'stat': 'POST /tripManager/fakeendpoint1',
        'metadata': {'key': 'val'}
    }

    self.archiver.insert(entry1)
    self.archiver.insert(entry2)
    savedEntries = self.archiver.query_readings()
    self.assertEquals(len(savedEntries), 2)
    entry = savedEntries[0]
    self.assertEquals(entry['Readings'], [[1417725167, 0.189722061157]])
    
    entry = savedEntries[1]
    self.assertEquals(entry['Readings'], [[1417725167, 0.36]])


class TestStats(unittest.TestCase):
  def setUp(self):
    get_client_stats_db().remove()
    get_server_stats_db().remove()
    get_result_stats_db().remove()
    self.testInputJSON = \
      {'Metadata': {'client_app_version': '2.0.1',
                    'client_os_version': '4.3'},
       'Readings': {'sync_pull_list_size': [[1411418998701, 1111], [1411418998702, 2222], [1411418998703, 3333]],
                 'battery_level': [[1411418998704, 4444], [1411418998705, 5555], [1411418998706, 6666]],
                }}

  def tearDown(self):
    get_client_stats_db().remove()
    pass

  def testCreateEntry(self):
    currEntry = stats.createEntry("testuser", "testkey", "testTs", "testVal")
    self.assertEquals(currEntry['user'], "testuser")
    self.assertEquals(currEntry['stat'], "testkey")
    self.assertEquals(currEntry['ts'], "testTs")

  def testSetClientMeasurements(self):
    currTime = time.time()
    stats.setClientMeasurements("testUser", self.testInputJSON)
    savedEntries = get_client_stats_db().query_tags()
    self.assertEquals(type(savedEntries), list)
    self.assertEquals(len(savedEntries), 2)


    # @TODO: Improve this test to be more informative
    for savedEntry in savedEntries:
      #print(savedEntry)
      self.assertEquals(savedEntry['Metadata']['client_app_version'], '2.0.1')
      self.assertEquals(savedEntry['Metadata']['client_os_version'], '4.3')
      #self.assertAlmostEqual(savedEntry['Metadata']['reported_ts'], time.time(), places = 0)
      #if savedEntry['stat'] == 'sync_pull_list_size':
      #  self.assertIn(savedEntry['ts'], [1411418998701, 1411418998702, 1411418998703])
      #  self.assertIn(savedEntry['reading'], [1111, 2222, 3333])

    #savedEntries = get_client_stats_db().query_readings()
    #self.assertEquals(len(savedEntries), 6)



  def testStoreClientEntry(self):
    currTime = time.time()
    self.assertEqual(get_client_stats_db().query_tags(), None)
    
    success = stats.storeClientEntry("testuser", "testfield", currTime, 0.002, {"metadata_key": "metadata_val"})
    savedEntries = get_client_stats_db().query_tags()
    self.assertEquals(type(savedEntries), list)
    self.assertEquals(len(savedEntries), 1)
    entry = savedEntries[0]
    self.assertEquals(entry["Metadata"]["metadata_key"], "metadata_val")
    #self.assertEqual(get_client_stats_db().find({'user': 'testuser'}).count(), 1)
    #self.assertEqual(get_client_stats_db().find({'ts': currTime}).count(), 1)


  def testStoreServerEntry(self):
    currTime = time.time()
    self.assertEqual(get_client_stats_db().query_tags(), None)
    success = stats.storeServerEntry("testuser", "GET foo", currTime, 0.002)
    self.assertEquals(success, True)
    savedEntries = get_server_stats_db().query_tags()
    self.assertEquals(type(savedEntries), list)
    self.assertEquals(len(savedEntries), 1)
    entry = savedEntries[0]
    self.assertEquals(entry["Metadata"]["user"], "testuser")
    #self.assertEqual(get_server_stats_db().find().count(), 1)
    #self.assertEqual(get_server_stats_db().find({'user': 'testuser'}).count(), 1)
    #self.assertEqual(get_server_stats_db().find({'ts': currTime}).count(), 1)
  
  def testClientMeasurementCount(self):
    self.assertEquals(stats.getClientMeasurementCount(self.testInputJSON['Readings']), 6)

if __name__ == '__main__':
    unittest.main()
