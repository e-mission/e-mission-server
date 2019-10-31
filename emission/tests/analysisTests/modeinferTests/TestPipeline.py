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
import os
from datetime import datetime, timedelta

# Our imports
import emission.core.get_database as edb
from emission.core.wrapper.user import User
from emission.core.wrapper.client import Client
import emission.core.wrapper.localdate as ecwl
import emission.core.wrapper.location as ecwlo
import emission.core.wrapper.section as ecws
import emission.core.wrapper.entry as ecwe
import emission.core.wrapper.modeprediction as ecwm
import emission.core.wrapper.motionactivity as ecwma

import emission.tests.common as etc
import emission.net.usercache.formatters.common as enufc

import emission.analysis.classification.inference.mode.pipeline as pipeline
import emission.analysis.classification.inference.mode.reset as modereset
import emission.storage.timeseries.abstract_timeseries as esta
import emission.storage.decorations.analysis_timeseries_queries as esda
import emission.storage.decorations.section_queries as esds

'''
TODO:
    Create some outliers and make sure they're stripped out

'''
class TestPipeline(unittest.TestCase):
  def setUp(self):
        # Thanks to M&J for the number!
      np.random.seed(61297777)
      self.copied_model_path = etc.copy_dummy_seed_for_inference()
      dataFile = "emission/tests/data/real_examples/shankari_2016-08-10"
      start_ld = ecwl.LocalDate({'year': 2016, 'month': 8, 'day': 9})
      end_ld = ecwl.LocalDate({'year': 2016, 'month': 8, 'day': 10})
      cacheKey = "diary/trips-2016-08-10"
      etc.setupRealExample(self, dataFile)
      etc.runIntakePipeline(self.testUUID)
      # Default intake pipeline now includes mode inference
      # this is correct in general, but causes errors while testing the mode inference
      # because then that step is effectively run twice. This code
      # rolls back the results of running the mode inference as part of the
      # pipeline and allows us to correctly test the mode inference pipeline again.
      modereset.del_objects_after(self.testUUID, 0, is_dry_run=False)
      self.pipeline = pipeline.ModeInferencePipeline()
      self.pipeline.loadModelStage()

  def tearDown(self):
        logging.debug("Clearing related databases")
        self.clearRelatedDb()
        os.remove(self.copied_model_path)

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
    self.pipeline.selFeatureIndices = self.pipeline.selectFeatureIndicesStep()
    self.assertEqual(len(self.pipeline.selFeatureIndices), 13)

  def testGenerateFeatureMatrixAndIds(self):
    self.testSelectFeatureIndicesStep()

    self.pipeline.user_id = self.testUUID
    self.pipeline.ts = esta.TimeSeries.get_time_series(self.testUUID)
    self.pipeline.toPredictSections = esda.get_entries(esda.CLEANED_SECTION_KEY, self.testUUID, 
        time_query=None)
    (self.pipeline.toPredictFeatureMatrix,
        self.pipeline.tripIds,
        self.pipeline.sectionIds) = \
        self.pipeline.generateFeatureMatrixAndIDsStep(self.pipeline.toPredictSections)
    self.assertEqual(self.pipeline.toPredictFeatureMatrix.shape[0], len(self.pipeline.sectionIds))
    self.assertEqual(self.pipeline.toPredictFeatureMatrix.shape[0], len(self.pipeline.tripIds))
    self.assertEqual(self.pipeline.toPredictFeatureMatrix.shape[1], len(self.pipeline.selFeatureIndices))
    self.assertEqual(self.pipeline.toPredictFeatureMatrix.shape[0], len(self.pipeline.toPredictSections))

  def testPredictedProb(self):
    self.testGenerateFeatureMatrixAndIds()

    self.pipeline.predictedProb = self.pipeline.predictModesStep()
    logging.debug("self.pipeline.predictedProb = %s" % 
        self.pipeline.predictedProb)
    self.assertEqual(self.pipeline.predictedProb.shape[0], len(self.pipeline.toPredictSections))
    # our simple static model has two results
    self.assertEqual(self.pipeline.predictedProb.shape[1], 2)

    # I know this from looking at the output for this small dataset
    # self.assertAlmostEqual(self.pipeline.predictedProb[0,0], 0.9, places=3)
    self.assertEqual(self.pipeline.predictedProb[0,0], 0.7)
    self.assertTrue(round(self.pipeline.predictedProb[0,1],2) == 0.3 or 
        round(self.pipeline.predictedProb[0,1],2) == 0.3,
        "predictedProb[0,1] = %s, with rounding = %s" % (self.pipeline.predictedProb[0,1],
            round(self.pipeline.predictedProb[0,1])))
    self.assertTrue(round(self.pipeline.predictedProb[2,0],2) == 0.2 or
        round(self.pipeline.predictedProb[2,0],2) == 0.2,
        "predictedProb[2,0] = %s, with rounding = %s" % (self.pipeline.predictedProb[2,0],
            round(self.pipeline.predictedProb[2,0])))
    self.assertTrue(round(self.pipeline.predictedProb[2,1],2) == 0.8 or
        round(self.pipeline.predictedProb[2,1],2) == 0.8,
        "predictedProb[2,1] = %s, with rounding = %s" % (self.pipeline.predictedProb[2,1],
            round(self.pipeline.predictedProb[2,1])))

  def testConvertPredictedProbToMap(self):
    self.testPredictedProb()

    uniqueModes = self.pipeline.model.classes_
    self.assertEqual(uniqueModes.tolist(), [1,5])

    currProb = self.pipeline.convertPredictedProbToMap(uniqueModes,
        self.pipeline.predictedProb[2])

    self.assertEqual(currProb, {'WALKING': 0.2, 'BUS': 0.8})

  def testSavePredictionsStep(self):
    self.testPredictedProb()
    self.pipeline.savePredictionsStep()
    # Confirm that the predictions are saved correctly

    for i, section in enumerate(self.pipeline.toPredictSections):
        predicted_mode = esds.get_inferred_mode_entry(self.testUUID, section.get_id())
        self.assertIsNotNone(predicted_mode)
        if i == 0:
            self.assertEqual(predicted_mode.data["predicted_mode_map"],
                {'WALKING': 0.7, 'BUS': 0.3})

        if i == 2:
            self.assertEqual(predicted_mode.data["predicted_mode_map"],
                {'WALKING': 0.2, 'BUS': 0.8})

  def testEntirePipeline(self):
    self.pipeline.user_id = self.testUUID
    self.pipeline.runPredictionPipeline(self.testUUID, None)

    for i, section in enumerate(self.pipeline.toPredictSections):
        predicted_mode = esds.get_inferred_mode_entry(self.testUUID, section.get_id())
        self.assertIsNotNone(predicted_mode)
        if i == 0:
            self.assertEqual(predicted_mode.data["predicted_mode_map"],
                {'WALKING': 0.7, 'BUS': 0.3})

        if i == 2:
            self.assertEqual(predicted_mode.data["predicted_mode_map"],
                {'WALKING': 0.2, 'BUS': 0.8})

  def testAirOverrideHack(self):
    self.testPredictedProb()
    self.pipeline.toPredictSections[1]["data"]["sensed_mode"] = ecwma.MotionTypes.AIR_OR_HSR.value
    self.pipeline.toPredictSections[3]["data"]["sensed_mode"] = ecwma.MotionTypes.AIR_OR_HSR.value
    self.pipeline.savePredictionsStep()

    for i, section in enumerate(self.pipeline.toPredictSections):
        predicted_mode = esds.get_inferred_mode_entry(self.testUUID, section.get_id())
        self.assertIsNotNone(predicted_mode)

        ise = esds.cleaned2inferred_section(self.testUUID, section.get_id())
        self.assertIsNotNone(ise)

        if i == 0:
            self.assertEqual(predicted_mode.data["predicted_mode_map"],
                {'WALKING': 0.7, 'BUS': 0.3})
            self.assertEqual(ise.data.sensed_mode, ecwm.PredictedModeTypes.WALKING)

        if i == 1:
            self.assertEqual(predicted_mode.data["predicted_mode_map"],
                {'AIR_OR_HSR': 1.0})
            self.assertEqual(ise.data.sensed_mode, ecwm.PredictedModeTypes.AIR_OR_HSR)

        if i == 2:
            self.assertEqual(predicted_mode.data["predicted_mode_map"],
                {'WALKING': 0.2, 'BUS': 0.8})
            self.assertEqual(ise.data.sensed_mode, ecwm.PredictedModeTypes.BUS)

        if i == 3:
            self.assertEqual(predicted_mode.data["predicted_mode_map"],
                {'AIR_OR_HSR': 1.0})
            self.assertEqual(ise.data.sensed_mode, ecwm.PredictedModeTypes.AIR_OR_HSR)

if __name__ == '__main__':
    etc.configLogging()
    unittest.main()
