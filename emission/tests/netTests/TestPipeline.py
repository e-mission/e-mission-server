import unittest
import logging
import arrow
import os

import emission.core.get_database as edb
import emission.core.wrapper.localdate as ecwl
import emission.tests.common as etc

from emission.net.api import pipeline

class TestPipeline(unittest.TestCase):
    def setUp(self):
        etc.setupRealExample(self,
                             "emission/tests/data/real_examples/shankari_2015-aug-21")
        self.testUUID1 = self.testUUID
        etc.setupRealExample(self,
                             "emission/tests/data/real_examples/shankari_2015-aug-27")
  
    def tearDown(self):
        self.clearRelatedDb()

    def clearRelatedDb(self):
        edb.get_timeseries_db().delete_many({"user_id": self.testUUID})
        edb.get_analysis_timeseries_db().delete_many({"user_id": self.testUUID})
        edb.get_pipeline_state_db().delete_many({"user_id": self.testUUID})
        edb.get_timeseries_db().delete_many({"user_id": self.testUUID1})
        edb.get_analysis_timeseries_db().delete_many({"user_id": self.testUUID1})
        edb.get_pipeline_state_db().delete_many({"user_id": self.testUUID1})

    def testNoAnalysisResults(self):
        self.assertEqual(pipeline.get_range(self.testUUID), (None, None))

    def testAnalysisResults(self):
        self.assertEqual(pipeline.get_range(self.testUUID), (None, None))
        etc.runIntakePipeline(self.testUUID)
        self.assertAlmostEqual(pipeline.get_range(self.testUUID), (1440688739.672, 1440729142.709))

if __name__ == '__main__':
    import emission.tests.common as etc
    etc.configLogging()
    unittest.main()
