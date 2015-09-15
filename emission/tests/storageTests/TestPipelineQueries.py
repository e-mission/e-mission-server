# Standard imports
import unittest
import datetime as pydt
import logging

# Our imports
import emission.core.get_database as edb
import emission.storage.pipeline_queries as epq
import emission.core.wrapper.pipelinestate as ewps

class TestPipelineQueries(unittest.TestCase):
    def setUp(self):
        edb.get_pipeline_state_db().remove()

    def tearDown(self):
        edb.get_pipeline_state_db().remove()

    def testStartProcessing(self):
        curr_state = epq.get_current_state(ewps.PipelineStages.USERCACHE)
        self.assertIsNone(curr_state)
        curr_query = epq.get_time_range_for_stage(ewps.PipelineStages.USERCACHE)
        self.assertEquals(curr_query.timeType, "write_ts")
        self.assertIsNone(curr_query.startTs)
        self.assertIsNotNone(curr_query.endTs)
        new_state = epq.get_current_state(ewps.PipelineStages.USERCACHE)
        self.assertIsNotNone(new_state)
        self.assertIsNotNone(new_state.curr_run_ts)
        self.assertNotIn("last_ts_run", new_state)

    def testStopProcessing(self):
        self.testStartProcessing()
        epq.mark_stage_done(ewps.PipelineStages.USERCACHE)
        final_state = epq.get_current_state(ewps.PipelineStages.USERCACHE)
        self.assertIsNotNone(final_state)
        self.assertIsNone(final_state.curr_run_ts)
        self.assertIsNotNone(final_state.last_ts_run)

    def testStartProcessingTwice(self):
        self.testStopProcessing()
        next_query = epq.get_time_range_for_stage(ewps.PipelineStages.USERCACHE)
        logging.debug("next_query = %s" % next_query)
        self.assertEquals(next_query.timeType, "write_ts")
        self.assertIsNotNone(next_query.startTs)
        self.assertIsNotNone(next_query.endTs)
        new_state = epq.get_current_state(ewps.PipelineStages.USERCACHE)
        self.assertIsNotNone(new_state)
        self.assertIsNotNone(new_state.curr_run_ts)
        self.assertIsNotNone(new_state.last_ts_run)

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    unittest.main()
