# Standard imports
import unittest
import logging
import json
import time

# Our imports
from emission.net.int_service.giles import archiver

logging.basicConfig(level=logging.DEBUG)

class TestArchiver(unittest.TestCase):
  def setUp(self):
    self.archiver = archiver.StatArchiver('test')

  def tearDown(self):
    self.archiver.remove()

  # @TODO: Rewrite this test, it doesn't really test much
  def testInsertEntryWithMetadata(self):
    entry = {
        'user': '3a307244-ecf1-3e6e-a9a7-3aaf101b40fa',
        'reading': 0.189722061157,
        'ts': 1417725167,
        'stat': 'POST /tripManager/getUnclassifiedSections',
        'metakey': 'metaval'
    }
    response = self.archiver.insert(entry)
    self.assertEqual(response, '')

  # @TODO: Rewrite this test, it doesn't really test much
  def testInsertEntryWithoutMetadata(self):
    entry = {
        'user': '3a307244-ecf1-3e6e-a9a7-3aaf101b40fa',
        'reading': 0.189722061157,
        'ts': 1417725167,
        'stat': 'POST /tripManager/getUnclassifiedSections'
    }
    response = self.archiver.insert(entry)
    self.assertEqual(response, '')


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
    self.assertEquals(entry['Metadata']['Collection'], self.archiver.collection)
    self.assertEquals(entry['Metadata']['user'], '3a307244-ecf1-3e6e-a9a7-3aaf101b40fa')
    self.assertEquals(entry['Metadata']['stat'], 'POST /tripManager/getUnclassifiedSections')
    self.assertEquals(entry['Metadata']['metakey'], 'metaval')

  def testQueryReadings(self):
    entry1 = {
        'user': '3a307244-ecf1-3e6e-a9a7-3aaf101b40fa',
        'reading': 0.189722061157,
        'ts': 1417725167,
        'stat': 'POST /tripManager/fakeendpoint1',
        'metakey': 'metaval'
    }

    entry2 = {
        'user': 'abcdefgh-ecf1-3e6e-a9a7-3aaf101b40fa',
        'reading': 0.36,
        'ts': 1417725167,
        'stat': 'POST /tripManager/fakeendpoint1',
        'metakey': 'metaval'
    }

    self.archiver.insert(entry1)
    self.archiver.insert(entry2)
    savedEntries = self.archiver.query_readings()
    self.assertEquals(len(savedEntries), 2)
    entry = savedEntries[0]
    self.assertEquals(entry['Readings'], [[1417725167, 0.189722061157]])
    
    entry = savedEntries[1]
    self.assertEquals(entry['Readings'], [[1417725167, 0.36]])

  def testBadEntry(self):
    entry1 = {
        'user': '3a307244-ecf1-3e6e-a9a7-3aaf101b40fa',
        'reading': 'abc',
        'ts': 'def',
        'stat': 'POST /tripManager/fakeendpoint1',
        'metakey': 'metaval'
    }    
    response = self.archiver.insert(entry1)
    self.assertEquals(response, None)

if __name__ == '__main__':
    unittest.main()

