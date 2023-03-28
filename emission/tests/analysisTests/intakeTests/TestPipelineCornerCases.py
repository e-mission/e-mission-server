import unittest
import logging
import uuid
import time

# Our imports
import emission.core.get_database as edb
import emission.core.wrapper.localdate as ecwl

import emission.net.usercache.abstract_usercache_handler as enuah
import emission.analysis.plotting.geojson.geojson_feature_converter as gfc
import emission.storage.timeseries.tcquery as estt
import emission.core.common as ecc
import emission.core.wrapper.pipelinestate as ewps
import emission.storage.pipeline_queries as epq

# Test imports
import emission.tests.common as etc

class TestPipelineRealData(unittest.TestCase):
    def setUp(self):
        self.testUUID = uuid.uuid4()
        logging.info("setUp complete")

    def tearDown(self):
        logging.debug("Clearing related databases for %s" % self.testUUID)
        # Clear the database only if it is not an evaluation run
        # A testing run validates that nothing has changed
        # An evaluation run compares to different algorithm implementations
        # to determine whether to switch to a new implementation
        self.clearRelatedDb()

    def clearRelatedDb(self):
        logging.info("Timeseries delete result %s" % edb.get_timeseries_db().delete_many({"user_id": self.testUUID}).raw_result)
        logging.info("Analysis delete result %s" % edb.get_analysis_timeseries_db().delete_many({"user_id": self.testUUID}).raw_result)
        logging.info("Usercache delete result %s" % edb.get_usercache_db().delete_many({"user_id": self.testUUID}).raw_result)
        logging.info("Usercache delete result %s" % edb.get_pipeline_state_db().delete_many({"user_id": self.testUUID}).raw_result)

    def testNoData(self):
        before_start = time.time()
        etc.runIntakePipeline(self.testUUID)
        after_run = time.time()
        logging.info(f"Finished running pipeline from {before_start} -> {after_run}")
        stages_skipped_in_testing = [
            ewps.PipelineStages.USERCACHE,
            ewps.PipelineStages.TRIP_MODEL,
            ewps.PipelineStages.TOUR_MODEL,
            ewps.PipelineStages.ALTERNATIVES,
            ewps.PipelineStages.USER_MODEL,
            ewps.PipelineStages.RECOMMENDATION,
            ewps.PipelineStages.OUTPUT_GEN]
        
        for pse in ewps.PipelineStages.__iter__():
            if pse in stages_skipped_in_testing:
                continue
            ps = epq.get_current_state(self.testUUID, pse)
            logging.info(f"Pipeline stage for {self.testUUID}, {pse} is {ps}")
            self.assertEqual(ps['user_id'], self.testUUID)
            self.assertIsNone(ps['curr_run_ts'])
            self.assertIsNone(ps['last_processed_ts'])
            self.assertIsNotNone(ps['last_ts_run'])
            self.assertGreater(ps['last_ts_run'], before_start - epq.END_FUZZ_AVOID_LTE)
            self.assertLess(ps['last_ts_run'], after_run)

    def testAllDefinedPipelineStates(self):
        all_pipeline_states = edb.get_pipeline_state_db().find()
        valid_states = [pse.value for pse in ewps.PipelineStages.__iter__()]
        for ps in all_pipeline_states:
            self.assertIsNotNone(ps['user_id'])
            self.assertIn(ps['pipeline_stage'], valid_states)


if __name__ == '__main__':
    etc.configLogging()
    unittest.main()
