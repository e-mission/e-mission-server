#Standard imports
import unittest
import json
import logging
import numpy as np
from datetime import datetime, timedelta
import os
from pymongo import MongoClient

# Our imports
from emission.core.get_database import get_mode_db, get_section_db
# This is the old "seed" pipeline
import emission.analysis.classification.inference.mode.seed.pipeline as pipeline
from emission.core.wrapper.user import User
from emission.core.wrapper.client import Client
import emission.tests.common as etc
import emission.core.get_database as edb

class TestPipelineSeed(unittest.TestCase):
  def setUp(self):
    self.testUsers = ["test@example.com", "best@example.com", "fest@example.com",
                      "rest@example.com", "nest@example.com"]
    self.serverName = edb.url

    self.ModesColl = get_mode_db()
    self.SectionsColl = get_section_db()

    # Let's make sure that the users are registered so that they have profiles
    user_objects = []
    for userEmail in self.testUsers:
      user_objects.append(User.register(userEmail))

    # Sometimes, we may have entries left behind in the database if one of the tests failed
    # or threw an exception, so let us start by cleaning up all entries
    for testUser in user_objects:
        etc.purgeSectionData(self.SectionsColl, testUser.uuid)

    if self.ModesColl.estimated_document_count() > 0:
        self.ModesColl.delete_many({})

    self.assertEqual(self.ModesColl.estimated_document_count(), 0)

    self.assertEqual(self.SectionsColl.estimated_document_count(), 0)

    MongoClient(edb.url).drop_database("Backup_database")

    etc.loadTable(self.serverName, "Stage_Modes", "emission/tests/data/modes.json")
    etc.loadTable(self.serverName, "Stage_Sections", "emission/tests/data/testModeInferSeedFile")

    self.now = datetime.now()
    self.dayago = self.now - timedelta(days=1)
    self.weekago = self.now - timedelta(weeks = 1)

    for section in self.SectionsColl.find():
      section['section_start_datetime'] = self.dayago
      section['section_end_datetime'] = self.dayago + timedelta(hours = 1)
      if (section['confirmed_mode'] == 5):
        # We only cluster bus and train trips
        # And our test data only has bus trips
        section['section_start_point'] = {u'type': u'Point', u'coordinates': [-122.270039042, 37.8800285728]}
        section['section_end_point'] = {u'type': u'Point', u'coordinates': [-122.2690412952, 37.8739578595]}
      # print("Section start = %s, section end = %s" %
      #   (section['section_start_datetime'], section['section_end_datetime']))
      # Replace the user email with the UUID
      section['user_id'] = User.fromEmail(section['user_id']).uuid
      edb.save(self.SectionsColl, section)

    self.pipeline = pipeline.ModeInferencePipelineMovesFormat()
    self.testLoadTrainingData()

  def tearDown(self):
    MongoClient(edb.url).drop_database("Backup_database")
    for testUser in self.testUsers:
      etc.purgeSectionData(self.SectionsColl, testUser)
    logging.debug("Number of sections after purge is %d" % self.SectionsColl.estimated_document_count())
    self.ModesColl.delete_many({})
    self.assertEqual(self.ModesColl.estimated_document_count(), 0)
    if os.path.exists(pipeline.SAVED_MODEL_FILENAME):
        os.remove(pipeline.SAVED_MODEL_FILENAME)
        self.assertFalse(os.path.exists(pipeline.SAVED_MODEL_FILENAME))

  def testLoadTrainingData(self):
    from pymongo import MongoClient

    allConfirmedTripsQuery = pipeline.ModeInferencePipelineMovesFormat.getSectionQueryWithGroundTruth({'$ne': ''})
    (self.pipeline.confirmedSectionCount, self.pipeline.confirmedSections) = self.pipeline.loadTrainingDataStep(allConfirmedTripsQuery)
    backupSections = MongoClient(edb.url).Backup_database.Stage_Sections
    (self.pipeline.backupSectionCount, self.pipeline.backupConfirmedSections) = self.pipeline.loadTrainingDataStep(allConfirmedTripsQuery, backupSections)
    self.assertEqual(self.pipeline.confirmedSectionCount, len(self.testUsers) * 2)
    self.assertEqual(self.pipeline.backupSectionCount, 0)
    

  def testGenerateBusAndTrainStops(self):
    (self.pipeline.bus_cluster, self.pipeline.train_cluster) = self.pipeline.generateBusAndTrainStopStep()
    # Half our trips are bus, and are copies of the identical bus trip.
    # So they should all cluster into one set of start and stop points.
    # So we expect to have to cluster points - one for start and one for end
    self.assertEqual(len(self.pipeline.train_cluster), 0)
    self.assertEqual(len(self.pipeline.bus_cluster), 2)

  def testFeatureGenWithOnePoint(self):
    trackpoint1 = {"track_location": {"coordinates": [-122.0861645, 37.3910201]},
                   "time" : "20150127T203305-0800"}
    now = datetime.now()

    # ensure that the start and end datetimes are the same, since the average calculation uses
    # the total distance and the total duration
    testSeg = {"track_points": [trackpoint1],
               "distance": 500,
               "section_start_datetime": now,
               "section_end_datetime": now,
               "mode": 1,
               "section_id": 2}

    featureMatrix = np.zeros([1, len(self.pipeline.featureLabels)])
    resultVector = np.zeros(1)
    self.pipeline.updateFeatureMatrixRowWithSection(featureMatrix, 0, testSeg)
    self.assertEqual(np.count_nonzero(featureMatrix[0][4:16]), 0)
    self.assertEqual(np.count_nonzero(featureMatrix[0][19:21]), 0)

  def testGenerateTrainingSet(self):
    self.testLoadTrainingData()
    self.testGenerateBusAndTrainStops()

    (self.pipeline.featureMatrix, self.pipeline.resultVector) = self.pipeline.generateFeatureMatrixAndResultVectorStep()
    logging.debug("Number of sections = %s" % self.pipeline.confirmedSectionCount)
    logging.debug("Feature Matrix shape = %s" % str(self.pipeline.featureMatrix.shape))
    self.assertEqual(self.pipeline.featureMatrix.shape[0], self.pipeline.confirmedSectionCount)
    self.assertEqual(self.pipeline.featureMatrix.shape[1], len(self.pipeline.featureLabels))

  def testCleanDataStep(self):
    # Add in some entries that should be cleaned by duplicating existing sections
    runSec = self.SectionsColl.find_one({'type':'move'})
    runSec['_id'] = 'clean_me_1'
    runSec['confirmed_mode'] = 2
    logging.debug("Inserting runSec %s" % runSec)
    self.SectionsColl.insert_one(runSec)

    # Outlier trip
    longTripSec = self.SectionsColl.find_one({'type':'move'})
    longTripSec['_id'] = 'clean_me_2'
    longTripSec['distance'] = 5000000
    logging.debug("Inserting longTripSec %s" % longTripSec)
    self.SectionsColl.insert_one(longTripSec)

    unknownTripSec = self.SectionsColl.find_one({'type':'move'})
    unknownTripSec['_id'] = 'clean_me_3'
    unknownTripSec['mode'] = 'airplane'
    logging.debug("Inserting unknownTripSec %s" % unknownTripSec)
    self.SectionsColl.insert_one(unknownTripSec)
    
    allConfirmedTripsQuery = {"$and": [{'type': 'move'}, {'confirmed_mode': {'$ne': ''}}]}
    (self.pipeline.confirmedSectionCount, self.pipeline.confirmedSections) = self.pipeline.loadTrainingDataStep(allConfirmedTripsQuery)
    self.testGenerateBusAndTrainStops()
    (self.pipeline.featureMatrix, self.pipeline.resultVector) = self.pipeline.generateFeatureMatrixAndResultVectorStep()

    (self.pipeline.cleanedFeatureMatrix, self.pipeline.cleanedResultVector) = self.pipeline.cleanDataStep()
    self.assertEqual(self.pipeline.cleanedFeatureMatrix.shape[0], self.pipeline.confirmedSectionCount + self.pipeline.backupSectionCount - 2)

  def testSelectFeatureIndicesStep(self):
    self.testCleanDataStep()

    self.pipeline.selFeatureIndices = self.pipeline.selectFeatureIndicesStep()
    self.assertEqual(len(self.pipeline.selFeatureIndices), 13)
    self.pipeline.selFeatureMatrix = self.pipeline.cleanedFeatureMatrix[:,self.pipeline.selFeatureIndices]
    self.assertEqual(self.pipeline.selFeatureMatrix.shape[1], len(self.pipeline.selFeatureIndices))

  def testBuildModelStep(self):
    self.testSelectFeatureIndicesStep()

    self.pipeline.model = self.pipeline.buildModelStep()
    from sklearn import model_selection
    scores = model_selection.cross_val_score(self.pipeline.model, self.pipeline.cleanedFeatureMatrix, self.pipeline.cleanedResultVector, cv=3)
    self.assertGreater(scores.mean(), 0.90)

  def testSaveModelStep(self):
    self.testBuildModelStep()
    self.pipeline.saveModelStep()

    with open(pipeline.SAVED_MODEL_FILENAME, "r") as fd:
        self.assertIsNotNone(fd)

  def testLoadModelStep(self):
    self.testSaveModelStep()

    self.pipeline.model = pipeline.ModeInferencePipelineMovesFormat.loadModel()
    from sklearn import model_selection
    scores = model_selection.cross_val_score(self.pipeline.model, self.pipeline.cleanedFeatureMatrix, self.pipeline.cleanedResultVector, cv=3)
    self.assertGreater(scores.mean(), 0.90)

  def setupTestTrips(self, sectionsColl):
    # Generate some test data by taking existing training data and stripping out the labels
    test_id_1 = self.SectionsColl.find_one({'confirmed_mode':1})
    test_id_1['_id'] = 'test_id_1'
    test_id_1['confirmed_mode'] = ''
    logging.debug("Inserting test_id_1 %s" % test_id_1)
    sectionsColl.insert_one(test_id_1)

    test_id_2 = self.SectionsColl.find_one({'confirmed_mode':5})
    test_id_2['_id'] = 'test_id_2'
    test_id_2['confirmed_mode'] = ''
    logging.debug("Inserting test_id_2 %s" % test_id_2)
    sectionsColl.insert_one(test_id_2)

  def testEntirePipeline(self):
    self.setupTestTrips(self.SectionsColl)
    self.setupTestTrips(MongoClient(edb.url).Backup_database.Stage_Sections)
    # Here, we only have 5 trips, so the pipeline looks for the backup training
    # set instead, which fails because there is no backup. So let's copy data from
    # the main DB to the backup DB to make this test pass
    self.pipeline.runPipeline()

    # Checks are largely the same as above
    self.pipeline.model = pipeline.ModeInferencePipelineMovesFormat.loadModel()
    from sklearn import model_selection
    scores = model_selection.cross_val_score(self.pipeline.model, self.pipeline.cleanedFeatureMatrix, self.pipeline.cleanedResultVector, cv=3)
    self.assertGreater(scores.mean(), 0.90)

if __name__ == '__main__':
    etc.configLogging()
    unittest.main()
