from __future__ import print_function
from __future__ import unicode_literals
from __future__ import division
from __future__ import absolute_import
#Standard imports
from future import standard_library
standard_library.install_aliases()
from builtins import str
from builtins import *
import unittest
import json
import arrow
import logging
import numpy as np
from datetime import datetime, timedelta

# Our imports
import emission.core.get_database as edb
from emission.core.wrapper.user import User
from emission.core.wrapper.client import Client
import emission.core.wrapper.localdate as ecwl
import emission.core.wrapper.location as ecwlo
import emission.core.wrapper.section as ecws
import emission.core.wrapper.entry as ecwe

import emission.tests.common as etc
import emission.net.usercache.formatters.common as enufc

import emission.analysis.classification.inference.mode.pipeline as pipeline
import emission.storage.timeseries.abstract_timeseries as esta
'''
TODO:
    Create some outliers and make sure they're stripped out

'''
class TestPipeline(unittest.TestCase):
  def setUp(self):
        # Thanks to M&J for the number!
      np.random.seed(61297777)
      dataFile = "emission/tests/data/real_examples/shankari_2016-08-10"
      start_ld = ecwl.LocalDate({'year': 2016, 'month': 8, 'day': 9})
      end_ld = ecwl.LocalDate({'year': 2016, 'month': 8, 'day': 10})
      cacheKey = "diary/trips-2016-08-10"
      etc.setupRealExample(self, dataFile)
      self.pipeline = pipeline.ModeInferencePipeline()

  def tearDown(self):
        logging.debug("Clearing related databases")
        self.clearRelatedDb()

  def clearRelatedDb(self):
        edb.get_timeseries_db().delete_many({"user_id": self.testUUID})
        edb.get_analysis_timeseries_db().delete_many({"user_id": self.testUUID})
        edb.get_usercache_db().delete_many({"user_id": self.testUUID})

  def testFeatureGenWithOnePoint(self):
    # ensure that the start and end datetimes are the same, since the average calculation uses
    # the total distance and the total duration
    ts = esta.TimeSeries.get_time_series(self.testUUID)
    trackpoint1 = ecwlo.Location({u'coordinates': [0,0], 'type': 'Point'})
    ts.insert_data(self.testUUID, "analysis/recreated_location", trackpoint1)
    testSeg = ecws.Section({"start_loc": trackpoint1,
                "end_loc": trackpoint1,
                "distance": 500,
                "sensed_mode": 1,
                "duration": 150,
                "start_ts": arrow.now().timestamp,
                "end_ts": arrow.now().timestamp,
                "_id": 2,
                "speeds":[],
                "distances":[],
                })
    testSegEntry = ecwe.Entry.create_entry(self.testUUID, "analysis/cleaned_section", testSeg)
    d = testSegEntry.data
    m = testSegEntry.metadata
    enufc.expand_start_end_data_times(d, m)
    testSegEntry["data"] = d
    testSegEntry["metadata"] = m
    inserted_id = ts.insert(testSegEntry)
    featureMatrix = np.zeros([1, len(self.pipeline.featureLabels)])
    resultVector = np.zeros(1)
    self.pipeline.updateFeatureMatrixRowWithSection(featureMatrix, 0, testSegEntry) 
    logging.debug("featureMatrix = %s" % featureMatrix)
    self.assertEqual(np.count_nonzero(featureMatrix[0][5:16]), 0)
    self.assertEqual(np.count_nonzero(featureMatrix[0][19:21]), 0)


  def testSelectFeatureIndicesStep(self):
    self.testCleanDataStep()

    self.pipeline.selFeatureIndices = self.pipeline.selectFeatureIndicesStep()
    self.assertEqual(len(self.pipeline.selFeatureIndices), 13)
    self.pipeline.selFeatureMatrix = self.pipeline.cleanedFeatureMatrix[:,self.pipeline.selFeatureIndices]
    self.assertEqual(self.pipeline.selFeatureMatrix.shape[1], len(self.pipeline.selFeatureIndices))

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

  def testEntirePipeline(self):
    self.setupTestTrips()
    # Here, we only have 5 trips, so the pipeline looks for the backup training
    # set instead, which fails because there is no backup. So let's copy data from
    # the main DB to the backup DB to make this test pass
    from pymongo import MongoClient
    client = MongoClient('localhost')
    client.drop_database("Backup_database")
    client.admin.command("copydb", fromdb="Stage_database", todb="Backup_database")
    self.pipeline.runPipeline()

    # Checks are largely the same as above
    test_id_1_sec = self.SectionsColl.find_one({'_id': 'test_id_1'})
    self.assertIsNotNone(test_id_1_sec['predicted_mode'])
    self.assertEquals(test_id_1_sec['predicted_mode'], {'walking': 1})
    self.assertIsNotNone(test_id_1_sec['distance'])

if __name__ == '__main__':
    etc.configLogging()
    unittest.main()
