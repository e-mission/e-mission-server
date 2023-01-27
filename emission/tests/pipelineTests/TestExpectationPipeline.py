import unittest
import numpy as np
import time
import arrow
import copy

import emission.storage.timeseries.timequery as estt
import emission.core.wrapper.labelprediction as ecwl
import emission.analysis.userinput.expectations as eaue
import emission.storage.decorations.analysis_timeseries_queries as esda
import emission.analysis.classification.inference.labels.pipeline as eacilp
import emission.analysis.classification.inference.labels.inferrers as eacili
import emission.core.get_database as edb
import emission.tests.common as etc
import emission.analysis.configs.expectation_notification_config as eace

class TestExpectationPipeline(unittest.TestCase):
    test_algorithms = {
            ecwl.AlgorithmTypes.PLACEHOLDER_3: eacili.placeholder_predictor_3
    }
    tz = "America/Chicago"
    contrived_dates = {  # Reused from TestExpectationNotificationConfig
        494: arrow.get("2021-05-01T20:00:00.000", tzinfo=tz),
        565: arrow.get("2021-06-01T20:00:00.000", tzinfo=tz),
        795: arrow.get("2021-06-08T20:00:00.000", tzinfo=tz),
        805: arrow.get("2021-09-04T20:00:00.000", tzinfo=tz),
        880: arrow.get("2023-02-03T20:00:00.000", tzinfo=tz),
        960: arrow.get("2023-02-12T20:00:00.000", tzinfo=tz) 
    }

    @staticmethod
    def fingerprint(trip):
        # See eacilp.placeholder_predictor_2 for an explanation of the "fingerprint" technique
        return trip["data"]["start_local_dt"]["hour"]*60+trip["data"]["start_local_dt"]["minute"]

    def setUp(self):
        self.test_options_stash = copy.copy(eace._test_options)
        eace._test_options = {
            "use_sample": True,
            "override_keylist": ["mode_confirm", "purpose_confirm"]
        }
        eace.reload_config()
        
        np.random.seed(61297777)
        self.reset_all()
        etc.setupRealExample(self, "emission/tests/data/real_examples/shankari_2015-07-22")
        self.run_pipeline(self.test_algorithms)
        time_range = estt.TimeQuery("metadata.write_ts", None, time.time())
        self.inferred_trips = esda.get_entries(esda.INFERRED_TRIP_KEY, self.testUUID, time_query=time_range)
        self.inferred_id_to_trip = {trip.get_id(): trip for trip in self.inferred_trips}
        self.expected_trips = esda.get_entries(esda.EXPECTED_TRIP_KEY, self.testUUID, time_query=time_range)

    def tearDown(self):
        self.reset_all()
        eace._test_options = self.test_options_stash
        eace.reload_config()

    def run_pipeline(self, algorithms):
        primary_algorithms_stash = eacilp.primary_algorithms
        eacilp.primary_algorithms = algorithms
        test_options_stash = copy.copy(eaue._test_options)
        eaue._test_options["preprocess_trip"] = lambda trip: self.preprocess(trip)
        etc.runIntakePipeline(self.testUUID)
        eacilp.primary_algorithms = primary_algorithms_stash
        eaue._test_options = test_options_stash
    
    def preprocess(self, trip):
        trip["data"]["end_ts"] = self.contrived_dates[self.fingerprint(trip)].float_timestamp
        trip["data"]["end_local_dt"]["timezone"] = self.tz

    def reset_all(self):
        etc.dropAllCollections(edb._get_current_db())
    
    # Tests that the fields from the inferred trip are carried over into the expected trip correctly
    def testPipelineIntegrity(self):
        self.assertGreaterEqual(len(self.expected_trips), 1)  # Make sure we've actually loaded trips
        self.assertEqual(len(self.expected_trips), len(self.inferred_trips))
        for expected_trip in self.expected_trips:
            inferred_id = expected_trip["data"]["inferred_trip"]
            self.assertIn(inferred_id, self.inferred_id_to_trip.keys())
            inferred_trip = self.inferred_id_to_trip[inferred_id]
            self.assertEqual(expected_trip["data"]["raw_trip"], inferred_trip["data"]["raw_trip"])
            self.assertEqual(expected_trip["data"]["cleaned_trip"], inferred_trip["data"]["cleaned_trip"])
            self.assertEqual(expected_trip["data"]["inferred_labels"], inferred_trip["data"]["inferred_labels"])
    
    def testRawAgainstAnswers(self):
        answers = {
            494: {"type": "randomDays", "value": 2},
            565: {"type": "all"},
            795: {"type": "none"},
            805: {"type": "all"},
            880: {"type": "all"},
            960: {"type": "randomFraction", "value": 0.05}
        }
        for trip in self.expected_trips:
            self.assertEqual(eace.get_expectation(trip), answers[self.fingerprint(trip)],
                "trip: %s with fingerprint %s" % (trip, self.fingerprint(trip)))

    def testProcessedAgainstAnswers(self):
        answers = {
            494: None,
            565: True,
            795: False,
            805: True,
            880: True,
            960: None
        }
        for trip in self.expected_trips:
            ans = answers[self.fingerprint(trip)]
            if ans is not None: self.assertEqual(trip["data"]["expectation"]["to_label"], ans, "trip: %s with fingerprint %s" % (trip, self.fingerprint(trip)))

    def testProcessedAgainstRaw(self):
        for trip in self.expected_trips:
            self.assertIn("expectation", trip["data"])
            raw_expectation = eace.get_expectation(trip)
            if raw_expectation["type"] == "none":
                self.assertEqual(trip["data"]["expectation"]["to_label"], False)
            elif raw_expectation["type"] == "all":
                self.assertEqual(trip["data"]["expectation"]["to_label"], True)
            else:
                print("Expectation behavior for "+str(raw_expectation)+" has not been implemented yet; not testing. Value is "+str(trip["data"]["expectation"]))
            # TODO: implement tests for the other configurable expectation types once they've been implemented

    def testConfidenceThreshold(self):
        answers = {
            494: 0.55,
            565: 0.65,
            795: 0.55,
            805: 0.65,
            880: 0.65,
            960: 0.55
        }
        for trip in self.expected_trips:
            self.assertTrue("confidence_threshold" in trip["data"])  # Existence
            self.assertAlmostEqual(trip["data"]["confidence_threshold"], answers[self.fingerprint(trip)]) # Correctness

def main():
    etc.configLogging()
    unittest.main()

if __name__ == "__main__":
    main()
