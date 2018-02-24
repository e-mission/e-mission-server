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
import emission.core.wrapper.localdate as ecwl
import emission.core.wrapper.location as ecwlo
import emission.core.wrapper.section as ecws
import emission.core.wrapper.entry as ecwe
import emission.core.wrapper.modeprediction as ecwm
import emission.core.wrapper.motionactivity as ecwma


'''
TODO:
    Create some outliers and make sure they're stripped out

'''
class TestPipeline(unittest.TestCase):
  def setUp(self):
      import emission.tests.common as etc
      import emission.net.usercache.formatters.common as enufc
  
      import emission.analysis.classification.inference.mode.pipeline as pipeline
      import emission.storage.timeseries.abstract_timeseries as esta
      import emission.storage.decorations.analysis_timeseries_queries as esda
      import emission.storage.decorations.section_queries as esds

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
      pipeline.del_objects_after(self.testUUID, 0, is_dry_run=False)
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
    import emission.tests.common as etc
    import emission.net.usercache.formatters.common as enufc
 
    import emission.analysis.classification.inference.mode.pipeline as pipeline
    import emission.storage.timeseries.abstract_timeseries as esta
    import emission.storage.decorations.analysis_timeseries_queries as esda
    import emission.storage.decorations.section_queries as esds
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

  def testEntirePipeline(self):
    self.pipeline.user_id = self.testUUID
    self.pipeline.runPredictionPipeline(self.testUUID, None)

if __name__ == '__main__':
    etc.configLogging()
    unittest.main()
