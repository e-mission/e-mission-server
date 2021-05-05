from __future__ import unicode_literals
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import
# Standard imports
from future import standard_library
standard_library.install_aliases()
from builtins import *
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
        edb.get_pipeline_state_db().delete_many({'user_id': self.testUUID})

    def tearDown(self):
        edb.get_pipeline_state_db().delete_many({'user_id': self.testUUID})

    def testStartProcessing(self):
        logging.debug("About to start processing")
        curr_state = epq.get_current_state(self.testUUID, ewps.PipelineStages.USERCACHE)
        self.assertIsNone(curr_state)
        curr_query = epq.get_time_range_for_stage(self.testUUID, ewps.PipelineStages.USERCACHE)
        logging.debug("curr_query = %s" % curr_query.get_query())
        self.assertEqual(curr_query.timeType, "metadata.write_ts")
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
        logging.debug("About to stop processing")
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
        logging.debug("About to start processing for the second time")
        next_query = epq.get_time_range_for_stage(self.testUUID, ewps.PipelineStages.USERCACHE)
        logging.debug("next_query = %s" % next_query.get_query())
        self.assertEqual(next_query.timeType, "metadata.write_ts")
        self.assertIsNotNone(next_query.startTs)
        self.assertIsNotNone(next_query.endTs)
        new_state = epq.get_current_state(self.testUUID, ewps.PipelineStages.USERCACHE)
        self.assertIsNotNone(new_state)
        self.assertIsNotNone(new_state.curr_run_ts)
        self.assertIsNotNone(new_state.last_ts_run)

    def testStartProcessingTwiceTwoStates(self):
        TEST_DONE_TS_BASE = 999999

        self.assertIsNone(epq.get_current_state(self.testUUID, ewps.PipelineStages.USERCACHE))
        self.assertIsNone(epq.get_current_state(self.testUUID, ewps.PipelineStages.TRIP_SEGMENTATION))
        self.assertIsNone(epq.get_current_state(self.testUUID, ewps.PipelineStages.SECTION_SEGMENTATION))

        logging.debug("About to start processing for the first time")
        logging.debug("starting stage usercache %s" % epq.get_time_range_for_stage(self.testUUID, ewps.PipelineStages.USERCACHE))
        logging.debug("starting stage trip_segmentation %s " % epq.get_time_range_for_stage(self.testUUID, ewps.PipelineStages.TRIP_SEGMENTATION))
        logging.debug("starting stage section_segmentation %s " % epq.get_time_range_for_stage(self.testUUID, ewps.PipelineStages.SECTION_SEGMENTATION))
        logging.debug("After first time processing, states = %s" % 
            list(edb.get_pipeline_state_db().find({"user_id": self.testUUID})))

        logging.debug("About to stop processing for the first time")
        epq.mark_stage_done(self.testUUID, ewps.PipelineStages.USERCACHE, TEST_DONE_TS_BASE)
        epq.mark_stage_done(self.testUUID, ewps.PipelineStages.TRIP_SEGMENTATION, TEST_DONE_TS_BASE + 1)
        epq.mark_stage_done(self.testUUID, ewps.PipelineStages.SECTION_SEGMENTATION, TEST_DONE_TS_BASE + 2)
        logging.debug("After first time stopping, states = %s" % 
            list(edb.get_pipeline_state_db().find({"user_id": self.testUUID})))

        logging.debug("About to start processing for the second time")
        logging.debug("starting stage usercache %s" % epq.get_time_range_for_stage(self.testUUID, ewps.PipelineStages.USERCACHE))
        logging.debug("starting stage trip_segmentation %s " % epq.get_time_range_for_stage(self.testUUID, ewps.PipelineStages.TRIP_SEGMENTATION))
        logging.debug("starting stage section_segmentation %s " % epq.get_time_range_for_stage(self.testUUID, ewps.PipelineStages.SECTION_SEGMENTATION))
        logging.debug("After second time starting, states = %s" % 
            list(edb.get_pipeline_state_db().find({"user_id": self.testUUID})))

        # First set of checks
        new_state = epq.get_current_state(self.testUUID, ewps.PipelineStages.USERCACHE)
        self.assertIsNotNone(new_state)
        self.assertIsNotNone(new_state.curr_run_ts)
        self.assertIsNotNone(new_state.last_ts_run)
        uc_ts = new_state.curr_run_ts

        new_state = epq.get_current_state(self.testUUID, ewps.PipelineStages.TRIP_SEGMENTATION)
        self.assertIsNotNone(new_state)
        self.assertIsNotNone(new_state.curr_run_ts)
        self.assertIsNotNone(new_state.last_ts_run)
        ts_ts = new_state.curr_run_ts

        new_state = epq.get_current_state(self.testUUID, ewps.PipelineStages.SECTION_SEGMENTATION)
        self.assertIsNotNone(new_state)
        self.assertIsNotNone(new_state.curr_run_ts)
        self.assertIsNotNone(new_state.last_ts_run)
        ss_ts = new_state.curr_run_ts

        logging.debug("About to stop processing for the second time")
        epq.mark_stage_done(self.testUUID, ewps.PipelineStages.USERCACHE, TEST_DONE_TS_BASE + 10)
        epq.mark_stage_done(self.testUUID, ewps.PipelineStages.TRIP_SEGMENTATION, TEST_DONE_TS_BASE + 11)
        epq.mark_stage_done(self.testUUID, ewps.PipelineStages.SECTION_SEGMENTATION, TEST_DONE_TS_BASE + 12)
        logging.debug("After second time stopping, states = %s" % 
            list(edb.get_pipeline_state_db().find({"user_id": self.testUUID})))

        new_state = epq.get_current_state(self.testUUID, ewps.PipelineStages.USERCACHE)
        self.assertIsNotNone(new_state)
        self.assertIsNone(new_state.curr_run_ts)
        self.assertEqual(new_state.last_ts_run, uc_ts)
        self.assertEqual(new_state.last_processed_ts, TEST_DONE_TS_BASE + 10)

        new_state = epq.get_current_state(self.testUUID, ewps.PipelineStages.TRIP_SEGMENTATION)
        self.assertIsNotNone(new_state)
        self.assertIsNone(new_state.curr_run_ts)
        self.assertEqual(new_state.last_ts_run, ts_ts)
        self.assertEqual(new_state.last_processed_ts, TEST_DONE_TS_BASE + 11)

        new_state = epq.get_current_state(self.testUUID, ewps.PipelineStages.SECTION_SEGMENTATION)
        self.assertIsNotNone(new_state)
        self.assertIsNone(new_state.curr_run_ts)
        self.assertEqual(new_state.last_ts_run, ss_ts)
        self.assertEqual(new_state.last_processed_ts, TEST_DONE_TS_BASE + 12)


if __name__ == '__main__':
    import emission.tests.common as etc
    etc.configLogging()
    unittest.main()
