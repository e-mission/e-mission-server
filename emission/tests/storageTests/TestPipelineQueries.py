# Standard imports
import unittest
import datetime as pydt
import logging
import uuid

# Our imports
import emission.core.get_database as edb
import emission.storage.pipeline_queries as epq
import emission.core.wrapper.pipelinestate as ewps

class TestPipelineQueries(unittest.TestCase):
    def setUp(self):
        self.testUUID = uuid.uuid4()
        edb.get_pipeline_state_db().remove()

    def tearDown(self):
        edb.get_pipeline_state_db().remove()

    def testStartProcessing(self):
        curr_state = epq.get_current_state(self.testUUID, ewps.PipelineStages.USERCACHE)
        self.assertIsNone(curr_state)
        curr_query = epq.get_time_range_for_stage(self.testUUID, ewps.PipelineStages.USERCACHE)
        self.assertEquals(curr_query.timeType, "metadata.write_ts")
        self.assertIsNone(curr_query.startTs)
        self.assertIsNotNone(curr_query.endTs)
        new_state = epq.get_current_state(self.testUUID, ewps.PipelineStages.USERCACHE)
        self.assertIsNotNone(new_state)
        self.assertIsNotNone(new_state.curr_run_ts)
        self.assertIsNone(new_state.last_ts_run)
        new_state_diff_user = epq.get_current_state(uuid.uuid4(), ewps.PipelineStages.USERCACHE)
        self.assertIsNone(new_state_diff_user)

    def testStopProcessing(self):
        self.testStartProcessing()
        TEST_DONE_TS = 999999
        epq.mark_stage_done(self.testUUID, ewps.PipelineStages.USERCACHE, TEST_DONE_TS)
        final_state = epq.get_current_state(self.testUUID, ewps.PipelineStages.USERCACHE)
        self.assertIsNotNone(final_state)
        self.assertIsNone(final_state.curr_run_ts)
        self.assertIsNotNone(final_state.last_ts_run)
        self.assertIsNotNone(final_state.last_processed_ts)
        self.assertIsNotNone(final_state.last_processed_ts, TEST_DONE_TS)

    def testFailProcessing(self):
        self.testStartProcessing()
        epq.mark_stage_failed(self.testUUID, ewps.PipelineStages.USERCACHE)
        final_state = epq.get_current_state(self.testUUID, ewps.PipelineStages.USERCACHE)
        self.assertIsNotNone(final_state)
        self.assertIsNone(final_state.curr_run_ts)
        self.assertIsNone(final_state.last_ts_run)

    def testStartProcessingTwice(self):
        self.testStopProcessing()
        next_query = epq.get_time_range_for_stage(self.testUUID, ewps.PipelineStages.USERCACHE)
        logging.debug("next_query = %s" % next_query)
        self.assertEquals(next_query.timeType, "write_ts")
        self.assertIsNotNone(next_query.startTs)
        self.assertIsNotNone(next_query.endTs)
        new_state = epq.get_current_state(self.testUUID, ewps.PipelineStages.USERCACHE)
        self.assertIsNotNone(new_state)
        self.assertIsNotNone(new_state.curr_run_ts)
        self.assertIsNotNone(new_state.last_ts_run)

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    unittest.main()
