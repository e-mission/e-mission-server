#Standard imports
import unittest
import json
import logging
import numpy as np
from datetime import datetime, timedelta

# Our imports
from emission.core.get_database import get_db, get_mode_db, get_section_db
import emission.analysis.classification.inference.mode as pipeline
from emission.core.wrapper.user import User
from emission.core.wrapper.client import Client
import emission.tests.common as etc
import emission.core.get_database as edb

'''
TODO:
    Create some outliers and make sure they're stripped out

'''
class TestPipeline(unittest.TestCase):
  def setUp(self):
    self.testUsers = ["test@example.com", "best@example.com", "fest@example.com",
                      "rest@example.com", "nest@example.com"]
    self.serverName = 'localhost'

    # Sometimes, we may have entries left behind in the database if one of the tests failed
    # or threw an exception, so let us start by cleaning up all entries
    etc.dropAllCollections(get_db())

    self.ModesColl = get_mode_db()
    self.assertEquals(self.ModesColl.find().count(), 0)

    self.SectionsColl = get_section_db()
    self.assertEquals(self.SectionsColl.find().count(), 0)

    etc.loadTable(self.serverName, "Stage_Modes", "emission/tests/data/modes.json")

    # Let's make sure that the users are registered so that they have profiles
    for userEmail in self.testUsers:
      User.register(userEmail)

    etc.setupRealExample(self, "emission/tests/data/newTestModeInferFileData")
    self.now = datetime.now()
    self.dayago = self.now - timedelta(days=1)
    self.weekago = self.now - timedelta(weeks = 1)

    for section in self.SectionsColl.find():
      section['section_start_datetime'] = self.dayago
      section['section_end_datetime'] = self.dayago + timedelta(hours = 1)
      if (('confirmed_mode' in section and section['confirmed_mode'] == 5) or ('sensed_mode' in section and section['sensed_mode'] == 5)):
        # We only cluster bus and train trips
        # And our test data only has bus trips
        section['section_start_point'] = {u'type': u'Point', u'coordinates': [-122.270039042, 37.8800285728]}
        section['section_end_point'] = {u'type': u'Point', u'coordinates': [-122.2690412952, 37.8739578595]}
      # Replace the user email with the UUID
      section['user_id'] = User.fromEmail(section['user_id']).uuid
      self.SectionsColl.save(section)

    self.pipeline = pipeline.ModeInferencePipeline()
    self.testLoadTrainingData()

  def tearDown(self):
    for testUser in self.testUsers:
      etc.purgeSectionData(self.SectionsColl, testUser)
    logging.debug("Number of sections after purge is %d" % self.SectionsColl.find().count())
    self.ModesColl.remove()
    self.assertEquals(self.ModesColl.find().count(), 0)

  def testLoadTrainingData(self):
    allConfirmedTripsQuery = pipeline.ModeInferencePipeline.getSectionQueryWithGroundTruth({'$ne': ''})
    (self.pipeline.modeList, self.pipeline.confirmedSections) = self.pipeline.loadTrainingDataStep(allConfirmedTripsQuery)
    
    self.assertEquals(self.pipeline.confirmedSections.count(), len(self.testUsers) * 2)

  def testGenerateBusAndTrainStops(self):
    (self.pipeline.bus_cluster, self.pipeline.train_cluster) = self.pipeline.generateBusAndTrainStopStep()
    # Half our trips are bus, and are copies of the identical bus trip.
    # So they should all cluster into one set of start and stop points.
    # So we expect to have to cluster points - one for start and one for end
    self.assertEquals(len(self.pipeline.train_cluster), 0)
    self.assertEquals(len(self.pipeline.bus_cluster), 2)


  def testFeatureGenWithOnePoint(self):
    trackpoint1 = {u'coordinates': [0,0]}
    now = datetime.now()

    # ensure that the start and end datetimes are the same, since the average calculation uses
    # the total distance and the total duration
    # testSeg = {"track_points": [trackpoint1],
    #            "distance": 500,
    #            "section_start_datetime": now,
    #            "section_end_datetime": now,
    #            "sensed_mode": 1,
    #            "section_id": 2}
    testSeg = {"start_loc": trackpoint1,
                "end_loc": trackpoint1,
                "distance": 500,
                "start_local_dt": {'hour': now.hour,
                                 "minute": now.minute,
                                  "second": now.second,
                                  "weekday": now.weekday(),
                                  "year": now.year, 
                                  "month": now.month,
                                  "day": now.day,
                                  "timezone": "America/Los_Angeles"},
                "end_local_dt": {"hour": now.hour,
                                 "minute": now.minute,
                                  "second": now.second,
                                  "weekday": now.weekday(),
                                  "year": now.year, 
                                  "month": now.month,
                                  "day": now.day,
                                  "timezone": "America/Los_Angeles"},
                "sensed_mode": 1,
                "duration": 150,
                "start_ts": (now - datetime(1970, 1, 1)).total_seconds(),
                "end_ts": (now - datetime(1970, 1, 1)).total_seconds(),
                "_id": 2,
                "speeds":[],
                "distances":[],
                }
    featureMatrix = np.zeros([1, len(self.pipeline.featureLabels)])
    resultVector = np.zeros(1)
    self.pipeline.updateFeatureMatrixRowWithSection(featureMatrix, 0, testSeg) 
    self.assertEqual(np.count_nonzero(featureMatrix[0][4:16]), 0)
    self.assertEqual(np.count_nonzero(featureMatrix[0][19:21]), 0)

  def testGenerateTrainingSet(self):
    self.testLoadTrainingData()
    self.testGenerateBusAndTrainStops()

    (self.pipeline.featureMatrix, self.pipeline.resultVector) = self.pipeline.generateFeatureMatrixAndResultVectorStep()
    self.assertEquals(self.pipeline.featureMatrix.shape[0], self.pipeline.confirmedSections.count())
    self.assertEquals(self.pipeline.featureMatrix.shape[1], len(self.pipeline.featureLabels))

  def testCleanDataStep(self):
    # Add in some entries that should be cleaned by duplicating existing sections
    #AAArunSec = self.SectionsColl.find_one({'type':'move'})
    allConfirmedTripsQuery = {'confirmed_mode': {'$ne': ''}}

    (self.pipeline.modeList, self.pipeline.confirmedSections) = self.pipeline.loadTrainingDataStep(allConfirmedTripsQuery)
    runSec = self.SectionsColl.find_one({'confirmed_mode' : {'$ne' : ''}})
    runSec['_id'] = 'clean_me_1'
    runSec['confirmed_mode'] = 2
    logging.debug("Inserting runSec %s" % runSec)
    self.SectionsColl.insert(runSec)

    # Outlier trip
    #AAAlongTripSec = self.SectionsColl.find_one({'type':'move'})
    longTripSec = self.SectionsColl.find_one({'confirmed_mode' : {'$ne' : ''}})

    longTripSec['_id'] = 'clean_me_2'
    longTripSec['distance'] = 5000000
    logging.debug("Inserting longTripSec %s" % longTripSec)
    self.SectionsColl.insert(longTripSec)

    unknownTripSec = self.SectionsColl.find_one({'confirmed_mode' : {'$ne' : ''}})
    unknownTripSec['_id'] = 'clean_me_3'
    #AAAunknownTripSec['mode'] = 'airplane'
    unknownTripSec['sensed_mode'] = 'airplane'
    logging.debug("Inserting unknownTripSec %s" % unknownTripSec)
    self.SectionsColl.insert(unknownTripSec)
    
    (self.pipeline.modeList, self.pipeline.confirmedSections) = self.pipeline.loadTrainingDataStep(allConfirmedTripsQuery)
    self.testGenerateBusAndTrainStops()
    (self.pipeline.featureMatrix, self.pipeline.resultVector) = self.pipeline.generateFeatureMatrixAndResultVectorStep()
    (self.pipeline.cleanedFeatureMatrix, self.pipeline.cleanedResultVector) = self.pipeline.cleanDataStep()
    self.assertEquals(self.pipeline.cleanedFeatureMatrix.shape[0], self.pipeline.confirmedSections.count() - 2)

  def testSelectFeatureIndicesStep(self):
    self.testCleanDataStep()

    self.pipeline.selFeatureIndices = self.pipeline.selectFeatureIndicesStep()
    self.assertEqual(len(self.pipeline.selFeatureIndices), 13)
    self.pipeline.selFeatureMatrix = self.pipeline.cleanedFeatureMatrix[:,self.pipeline.selFeatureIndices]
    self.assertEqual(self.pipeline.selFeatureMatrix.shape[1], len(self.pipeline.selFeatureIndices))

  def testBuildModelStep(self):
    self.testSelectFeatureIndicesStep()

    self.pipeline.model = self.pipeline.buildModelStep()
    from sklearn import cross_validation
    scores = cross_validation.cross_val_score(self.pipeline.model, self.pipeline.cleanedFeatureMatrix, self.pipeline.cleanedResultVector, cv=3)
    self.assertGreater(scores.mean(), 0.90)

  def setupTestTrips(self):
    # Generate some test data by taking existing training data and stripping out the labels
    test_id_1 = self.SectionsColl.find_one({'confirmed_mode':1})
    #BBBtest_id_1 = self.SectionsColl.find_one({'sensed_mode':1})

    test_id_1['_id'] = 'test_id_1'
    #BBBtest_id_1['sensed_mode'] = ''
    test_id_1['confirmed_mode'] = ''

    logging.debug("Inserting test_id_1 %s" % test_id_1)
    self.SectionsColl.insert(test_id_1)

    #BBBtest_id_2 = self.SectionsColl.find_one({'sensed_mode': 4})  #There are none in the test data that are sensed_mode 5, so I changed it to 4
    test_id_2 = self.SectionsColl.find_one({'confirmed_mode':5})

    test_id_2['_id'] = 'test_id_2'
    #BBBtest_id_2['sensed_mode'] = ''
    test_id_2['confirmed_mode'] = ''
    logging.debug("Inserting test_id_2 %s" % test_id_2)
    self.SectionsColl.insert(test_id_2)

  def testGenerateFeatureMatrixAndIds(self):
    self.setupTestTrips()
    self.testBuildModelStep()
    toPredictTripsQuery = {"$and": [{'confirmed_mode': ''},
    # TODO: Change to does not exist
      {'predicted_mode': None}]}

    (self.pipeline.toPredictFeatureMatrix, self.pipeline.sectionIds, self.pipeline.sectionUserIds) = self.pipeline.generateFeatureMatrixAndIDsStep(toPredictTripsQuery)
    self.assertEqual(self.pipeline.toPredictFeatureMatrix.shape[0], len(self.pipeline.sectionIds))
    self.assertEqual(self.pipeline.toPredictFeatureMatrix.shape[0], len(self.pipeline.sectionUserIds))
    self.assertEqual(self.pipeline.toPredictFeatureMatrix.shape[1], len(self.pipeline.selFeatureIndices))
    self.assertEqual(self.pipeline.toPredictFeatureMatrix.shape[0], 2)

  def testPredictedProb(self):
    self.testGenerateFeatureMatrixAndIds()

    self.pipeline.predictedProb = self.pipeline.predictModesStep()
    self.assertEqual(self.pipeline.predictedProb.shape[0], 2)
    self.assertEqual(self.pipeline.predictedProb.shape[1], len(set(self.pipeline.cleanedResultVector)))

    # I know this from looking at the output for this small dataset
    # self.assertAlmostEqual(self.pipeline.predictedProb[0,0], 0.9, places=3)
    self.assertEqual(self.pipeline.predictedProb[0,0], 1)
    logging.debug("predicted prob = %s" % self.pipeline.predictedProb)
    self.assertTrue(round(self.pipeline.predictedProb[0,1],2) == 0 or 
        round(self.pipeline.predictedProb[0,1],2) == 0.1,
        "predictedProb[0,1] = %s, with rounding = %s" % (self.pipeline.predictedProb[0,1],
            round(self.pipeline.predictedProb[0,1])))
    self.assertTrue(round(self.pipeline.predictedProb[1,0],2) == 0 or
        round(self.pipeline.predictedProb[1,0],2) == 0.1,
        "predictedProb[1,0] = %s, with rounding = %s" % (self.pipeline.predictedProb[1,0],
            round(self.pipeline.predictedProb[1,0])))
    self.assertTrue(round(self.pipeline.predictedProb[1,1],2) == 1 or
        round(self.pipeline.predictedProb[1,1],2) == 0.9,
        "predictedProb[1,1] = %s, with rounding = %s" % (self.pipeline.predictedProb[1,1],
            round(self.pipeline.predictedProb[1,1])))

  def testConvertPredictedProbToMap(self):
    self.testPredictedProb()

    uniqueModes = sorted(set(self.pipeline.cleanedResultVector))
    self.assertEquals(uniqueModes, [1,5])

    currProb = self.pipeline.convertPredictedProbToMap(self.pipeline.modeList,
      uniqueModes, self.pipeline.predictedProb[1])

    self.assertEquals(currProb, {'bus': 1})

  def testSavePredictionsStep(self):
    self.testPredictedProb()
    self.pipeline.savePredictionsStep()
    # Confirm that the predictions are saved correctly

    test_id_1_sec = self.SectionsColl.find_one({'_id': 'test_id_1'})
    self.assertIsNotNone(test_id_1_sec['predicted_mode'])
    self.assertEquals(test_id_1_sec['predicted_mode'], {'walking': 1})

    test_id_2_sec = self.SectionsColl.find_one({'_id': 'test_id_2'})
    self.assertIsNotNone(test_id_2_sec['predicted_mode'])
    self.assertEquals(test_id_2_sec['predicted_mode'], {'bus': 1})

    # Let's make sure that we didn't accidentally mess up other fields
    self.assertIsNotNone(test_id_1_sec['distance'])
    self.assertIsNotNone(test_id_2_sec['trip_id'])

  def testSavePredictionsStepWithClient(self):
    from emission.core.wrapper.user import User

    fakeEmail = "fest@example.com"

    client = Client("testclient")
    client.update(createKey = False)
    etc.makeValid(client)

    (resultPre, resultReg) = client.preRegister("this_is_the_super_secret_id", fakeEmail)
    self.assertEqual(resultPre, 0)
    self.assertEqual(resultReg, 1)

    user = User.fromEmail(fakeEmail)
    self.assertEqual(user.getFirstStudy(), 'testclient')

    self.testPredictedProb()
    self.pipeline.savePredictionsStep()
    # Confirm that the predictions are saved correctly

    test_id_1_sec = self.SectionsColl.find_one({'_id': 'test_id_1'})
    self.assertIsNotNone(test_id_1_sec['predicted_mode'])
    self.assertEquals(test_id_1_sec['predicted_mode'], {'walking': 1})
    self.assertEquals(test_id_1_sec['test_auto_confirmed'], {'mode': 1, 'prob': 1.0})

    test_id_2_sec = self.SectionsColl.find_one({'_id': 'test_id_2'})
    self.assertIsNotNone(test_id_2_sec['predicted_mode'])
    self.assertEquals(test_id_2_sec['predicted_mode'], {'bus': 1})
    self.assertEquals(test_id_2_sec['test_auto_confirmed'], {'mode': 5, 'prob': 1.0})

    # Let's make sure that we didn't accidentally mess up other fields
    self.assertIsNotNone(test_id_1_sec['distance'])
    self.assertIsNotNone(test_id_2_sec['trip_id'])

  def testEntirePipeline(self):
    self.setupTestTrips()
    # Here, we only have 5 trips, so the pipeline looks for the backup training
    # set instead, which fails because there is no backup. So let's copy data from
    # the main DB to the backup DB to make this test pass
    from pymongo import MongoClient
    MongoClient('localhost').drop_database("Backup_database")
    MongoClient('localhost').copy_database("Stage_database","Backup_database","localhost")
    self.pipeline.runPipeline()

    # Checks are largely the same as above
    test_id_1_sec = self.SectionsColl.find_one({'_id': 'test_id_1'})
    self.assertIsNotNone(test_id_1_sec['predicted_mode'])
    self.assertEquals(test_id_1_sec['predicted_mode'], {'walking': 1})
    self.assertIsNotNone(test_id_1_sec['distance'])

if __name__ == '__main__':
    etc.configLogging()
    unittest.main()
