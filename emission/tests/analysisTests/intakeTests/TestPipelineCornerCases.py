import unittest
from unittest.mock import MagicMock, patch
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
import emission.core.wrapper.user as ecwu
import emission.storage.pipeline_queries as epq
import emission.pipeline.intake_stage as epi

# Test imports
import emission.tests.common as etc

class TestPipelineCornerCases(unittest.TestCase):
    def setUp(self):
        self.testUUID = uuid.uuid4()
        # this happens consistently during user creation now, and we rely on it to run the pipeline
        print(f"Created profile {ecwu.User._createInitialProfile(self.testUUID)} for {self.testUUID=}")
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

    def testSkipPipelineNoNewEntries(self):
        # we start with no pipeline states for this user
        all_pipeline_states = edb.get_pipeline_state_db().find()
        stages_skipped_in_testing = [
            ewps.PipelineStages.USERCACHE,
            ewps.PipelineStages.TRIP_MODEL,
            ewps.PipelineStages.TOUR_MODEL,
            ewps.PipelineStages.ALTERNATIVES,
            ewps.PipelineStages.USER_MODEL,
            ewps.PipelineStages.RECOMMENDATION,
            ewps.PipelineStages.OUTPUT_GEN]
        test_run_states = list([pse.value for pse in
            filter(lambda pse: pse not in stages_skipped_in_testing,
                ewps.PipelineStages.__iter__())])
        curr_user_states = list(filter(lambda ps: ps["user_id"] == self.testUUID,
            all_pipeline_states))
        self.assertEqual(len(curr_user_states), 0)
        # next, we run the real pipeline, and end up with no entries
        # and we have an early return in that case
        print("-" * 10, "Running real pipeline on empty DB, expecting no change", "-" * 10)
        # We manually set the end ts to be after the last location (ts=3650), set in emission.test.common,
        # so that the pipeline will be skipped
        edb.get_profile_db().update_one({"user_id": self.testUUID},
                                        {"$set": {"pipeline_range.end_ts": 7 * 60 * 60}})
        print("-" * 10, f"Before running but after update, profile is {edb.get_profile_db().find_one({'user_id': self.testUUID})}", "-" * 10)
        epi.run_intake_pipeline_for_user(self.testUUID)
        all_pipeline_states = edb.get_pipeline_state_db().find()
        curr_user_states = list(filter(lambda ps: ps["user_id"] == self.testUUID,
            all_pipeline_states))
        self.assertEqual(len(curr_user_states), 0)
        # Now we load some data and run the test pipeline
        # which will generate some pipeline states
        print("-" * 10, "Running test pipeline on real data, expecting states to be set", "-" * 10)
        etc.setupRealExample(self, "emission/tests/data/real_examples/shankari_2016-07-25")
        # we force run the pipeline by setting profile back so we will run the pipeline this time
        edb.get_profile_db().update_one({"user_id": self.testUUID},
                                        {"$set": {"pipeline_range.end_ts": None}})
        epi.run_intake_pipeline_for_user(self.testUUID)

        all_pipeline_states_after_run = edb.get_pipeline_state_db().find()
        curr_user_states_after_run = list(filter(lambda ps: ps["user_id"] == self.testUUID,
            all_pipeline_states_after_run))
        states_set = [ps['pipeline_stage'] for ps in curr_user_states_after_run]
        print("-" * 10, "test run stages are ", test_run_states, "-" * 10)
        print("-" * 10, "states that are set are ", states_set, "-" * 10)
        self.assertGreater(len(curr_user_states_after_run), 0)
        self.assertEqual(sorted(states_set), sorted(test_run_states))
        # then we run the real pipeline again
        # We expect to see no changes between the first and the second run
        # because of the usercache skip
        # We manually set the end ts to be after the last location (ts=3650), set in emission.test.common,
        # so that the pipeline will be skipped
        edb.get_profile_db().update_one({"user_id": self.testUUID},
                                        {"$set": {"pipeline_range.end_ts": 7 * 60 * 60}})
        epi.run_intake_pipeline_for_user(self.testUUID)
        all_pipeline_states_after_test_run = edb.get_pipeline_state_db().find()
        curr_user_states_after_test_run = list(filter(lambda ps: ps["user_id"] == self.testUUID,
            all_pipeline_states_after_test_run))
        get_last_processed = lambda ps: ps['last_processed_ts']
        get_last_run = lambda ps: ps['last_ts_run']
        self.assertEqual(list(map(get_last_processed, curr_user_states_after_run)),
            list(map(get_last_processed, curr_user_states_after_test_run)))
        self.assertEqual(list(map(get_last_run, curr_user_states_after_run)),
            list(map(get_last_run, curr_user_states_after_test_run)))

        # we force run the pipeline by setting profile back so we will run the pipeline this time
        edb.get_profile_db().update_one({"user_id": self.testUUID},
                                        {"$set": {"pipeline_range.end_ts": None}})
        # then we run the real pipeline again with skip=False
        # We expect to see no changes between the first and the second run
        # because of the usercache skip
        epi.run_intake_pipeline_for_user(self.testUUID)
        all_pipeline_states_after_test_run = edb.get_pipeline_state_db().find()
        curr_user_states_after_test_run = list(filter(lambda ps: ps["user_id"] == self.testUUID,
            all_pipeline_states_after_test_run))
        get_last_processed = lambda ps: ps['last_processed_ts']
        get_last_run = lambda ps: ps['last_ts_run']
        self.assertEqual(list(map(get_last_processed, curr_user_states_after_run)),
            list(map(get_last_processed, curr_user_states_after_test_run)))
        self.assertNotEqual(list(map(get_last_run, curr_user_states_after_run)),
            list(map(get_last_run, curr_user_states_after_test_run)))
 

    @patch("emission.pipeline.intake_stage.edb.get_profile_db")
    @patch("emission.pipeline.intake_stage.edb.get_pipeline_state_db")
    @patch("emission.pipeline.intake_stage.euah.UserCacheHandler.getUserCacheHandler")
    @patch("emission.pipeline.intake_stage.eaum.match_incoming_user_inputs")
    def testRunIfNotInProgressUser(self, mock_match_inputs, mock_get_user_cache, mock_get_pipeline_state_db, mock_get_profile_db):
        # Mocking user profile to simulate an active user
        mock_get_profile_db.return_value.find_one.return_value = {
            "user_id": "test_uuid",
            "last_location_ts": 7 * 60 * 60,
            "pipeline_range": {"end_ts": None}
        }

        # Mocking pipeline state to simulate no in-progress stages
        mock_get_pipeline_state_db.return_value.count_documents.return_value = 0

        # Mocking UserCacheHandler
        mock_user_cache_handler = MagicMock()
        mock_get_user_cache.return_value = mock_user_cache_handler

        # Run the function
        epi.run_intake_pipeline_for_user("test_uuid")

        mock_match_inputs.assert_called()
        mock_user_cache_handler.moveToLongTerm.assert_called()

    @patch("emission.pipeline.intake_stage.edb.get_profile_db")
    @patch("emission.pipeline.intake_stage.edb.get_pipeline_state_db")
    @patch("emission.pipeline.intake_stage.euah.UserCacheHandler.getUserCacheHandler")
    @patch("emission.pipeline.intake_stage.eaum.match_incoming_user_inputs")
    def testReturnIfInProgressuser(self, mock_match_inputs, mock_get_user_cache, mock_get_pipeline_state_db, mock_get_profile_db):
        # Mock a non-dormant user so we will get to the next check
        mock_get_profile_db.return_value.find_one.return_value = {
            "user_id": "test_uuid",
            "last_location_ts": 7 * 60 * 60,
            "pipeline_range": {"end_ts": None}
        }

        # Mocking pipeline state to simulate in-progress stages
        mock_get_pipeline_state_db.return_value.count_documents.return_value = 1

        mock_user_cache_handler = MagicMock()
        mock_get_user_cache.return_value = mock_user_cache_handler

        epi.run_intake_pipeline_for_user("test_uuid")

        # Assert that the pipeline skipped processing for in-progress stages
        mock_match_inputs.assert_not_called()
        mock_user_cache_handler.moveToLongTerm.assert_not_called()

    # Test without mocks
    def testRunInParallel(self):
        all_pipeline_states = edb.get_pipeline_state_db().find()

        initial_last_runs = \
            [ps["last_ts_run"] for ps in edb.get_pipeline_state_db().find({"user_id": self.testUUID})]
        print(initial_last_runs)

        print("-" * 10, "Running test pipeline on real data, expecting states to be set", "-" * 10)
        etc.setupRealExample(self, "emission/tests/data/real_examples/shankari_2016-07-25")
        # we force run the pipeline by setting profile back so we will run the pipeline this time
        edb.get_profile_db().update_one({"user_id": self.testUUID},
                                        {"$set": {"pipeline_range.end_ts": None}})
        epi.run_intake_pipeline_for_user(self.testUUID)
        after_first_run = edb.get_profile_db().find_one({"user_id": self.testUUID})
        # 1469493031.0 is the number I got when running the test for the first time
        self.assertEqual(after_first_run.get("pipeline_range", {}).get("end_ts", None), 1469493031.0)

        first_round_last_runs = \
            [ps["last_ts_run"] for ps in edb.get_pipeline_state_db().find({"user_id": self.testUUID})]
        print(first_round_last_runs)

        # force the user to be active again
        edb.get_profile_db().update_one({"user_id": self.testUUID},
                                        {"$set": {"pipeline_range.end_ts": None}})

        # force set section segmentation's curr_run_ts
        edb.get_pipeline_state_db().update_one(
            {"user_id": self.testUUID,
            "pipeline_stage": ewps.PipelineStages.SECTION_SEGMENTATION.value},
            {"$set": {"curr_run_ts": 3600}}
        )

        epi.run_intake_pipeline_for_user(self.testUUID)

        new_last_runs = \
            [ps["last_ts_run"] for ps in edb.get_pipeline_state_db().find({"user_id": self.testUUID})]

        # Since we should have bailed out when there was already an active run
        self.assertEqual(first_round_last_runs, new_last_runs)

if __name__ == '__main__':
    etc.configLogging()
    unittest.main()
