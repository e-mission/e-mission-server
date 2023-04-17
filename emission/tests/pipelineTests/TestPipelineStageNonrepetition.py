# https://github.com/e-mission/e-mission-docs/issues/654 shows that it's easy to mess up pipeline stages such that they act on trips they've already acted on.
# Let's write a test to make sure that doesn't happen again.
# Lots of boilerplate here copypasted from TestExpectationPipeline. TODO: refactor that out.

import unittest
import numpy as np
import copy

import emission.tests.common as etc
import emission.core.get_database as edb
import emission.core.wrapper.labelprediction as ecwl
import emission.analysis.classification.inference.labels.pipeline as eacilp
import emission.analysis.classification.inference.labels.inferrers as eacili
import emission.analysis.configs.expectation_notification_config as eace
import emission.analysis.userinput.expectations as eaue

class TestPipelineStageNonrepetition(unittest.TestCase):
    # The limitation of this approach is that one must manually add database keys here.
    # So when implementing a new pipeline stage, put the relevant key(s) here!
    # I've tried to go through the various existing pipeline stages and put some of their keys here, but this may already be incomplete.
    # If a stage writes to the database with multiple keys, not all keys must necessarily be included.
    keys_to_track = [
        "segmentation/raw_place",
        "segmentation/raw_trip",
        "segmentation/raw_stop",
        "analysis/smoothing",
        "analysis/cleaned_trip",
        "analysis/inferred_section",
        "analysis/inferred_trip",
        "analysis/expected_trip",
        "analysis/confirmed_trip"
    ]

    def setUp(self):
        self.test_options_stash = copy.copy(eace._test_options)
        eace._test_options = {
            "use_sample": True,
            "override_keylist": None
        }
        eace.reload_config()
        
        np.random.seed(61297777)
        self.reset_all()
        etc.setupRealExample(self, "emission/tests/data/real_examples/shankari_2015-07-22")
        self.run_pipeline()

    def tearDown(self):
        self.reset_all()
        eace._test_options = self.test_options_stash
        eace.reload_config()

    def run_pipeline(self, algorithms={ecwl.AlgorithmTypes.PLACEHOLDER_2: eacili.placeholder_predictor_2}):
        primary_algorithms_stash = eacilp.primary_algorithms
        eacilp.primary_algorithms = algorithms
        test_options_stash = copy.copy(eaue._test_options)
        etc.runIntakePipeline(self.testUUID)  # testUUID is set in setupRealExample
        eacilp.primary_algorithms = primary_algorithms_stash
        eaue._test_options = test_options_stash

    def reset_all(self):
        etc.dropAllCollections(edb._get_current_db())
    
    def count_keys(self):
        counts = {}
        db = edb.get_analysis_timeseries_db()
        for key in self.keys_to_track:
            count = db.count_documents({"metadata.key": key, "user_id": self.testUUID})
            counts[key] = count
        # print(counts)
        return counts

    def testPipelineStageNonrepetition(self):
        before_keys = self.count_keys()
        self.run_pipeline()
        after_keys = self.count_keys()
        for key in self.keys_to_track:
            self.assertEqual(before_keys[key], after_keys[key], key)

def main():
    etc.configLogging()
    unittest.main()

if __name__ == "__main__":
    main()
