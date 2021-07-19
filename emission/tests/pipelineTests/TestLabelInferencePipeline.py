# This tests the label inference pipeline. It uses fake data and placeholder inference algorithms
# and thus intentionally does not test the real inference algorithms themselves.
import unittest
import numpy as np
import time

import emission.analysis.classification.inference.labels.pipeline as eacilp
import emission.core.wrapper.labelprediction as ecwl
import emission.storage.decorations.analysis_timeseries_queries as esda
import emission.storage.timeseries.timequery as estt
import emission.storage.decorations.trip_queries as esdt
import emission.core.get_database as edb
import emission.tests.common as etc

class TestLabelInferencePipeline(unittest.TestCase):
    # It is important that these functions be deterministic
    test_algorithms = {
            ecwl.AlgorithmTypes.PLACEHOLDER_0: eacilp.placeholder_predictor_0,
            ecwl.AlgorithmTypes.PLACEHOLDER_2: eacilp.placeholder_predictor_2
    }

    def setUp(self):
        np.random.seed(61297777)
        self.reset_all()
        # Note that shankari_2015-07-22 is different from shankari_2015-jul-22 and that if you use shankari_2015-jul-22 things will fail silently!!! Took me a lot of time to find that bug.
        etc.setupRealExample(self, "emission/tests/data/real_examples/shankari_2015-07-22")
        self.run_pipeline(self.test_algorithms)
        time_range = estt.TimeQuery("metadata.write_ts", None, time.time())
        self.cleaned_trips = esda.get_entries(esda.CLEANED_TRIP_KEY, self.testUUID, time_query=time_range)
        self.cleaned_id_to_trip = {trip.get_id(): trip for trip in self.cleaned_trips}
        self.inferred_trips = esda.get_entries(esda.INFERRED_TRIP_KEY, self.testUUID, time_query=time_range)

    def tearDown(self):
        self.reset_all()

    def run_pipeline(self, algorithms):
        default_primary_algorithms = eacilp.primary_algorithms
        eacilp.primary_algorithms = algorithms
        etc.runIntakePipeline(self.testUUID)
        eacilp.primary_algorithms = default_primary_algorithms

    def reset_all(self):
        etc.dropAllCollections(edb._get_current_db())

    # Tests that the fields from the cleaned trip are carried over into the inferred trip correctly
    def testPipelineIntegrity(self):
        self.assertGreaterEqual(len(self.inferred_trips), 1)  # Make sure we've actually loaded trips
        self.assertEqual(len(self.inferred_trips), len(self.cleaned_trips))
        for inferred_trip in self.inferred_trips:
            cleaned_id = inferred_trip["data"]["cleaned_trip"]
            self.assertIn(cleaned_id, self.cleaned_id_to_trip.keys())
            cleaned_trip = self.cleaned_id_to_trip[cleaned_id]
            self.assertEqual(inferred_trip["data"]["raw_trip"], cleaned_trip["data"]["raw_trip"])
            self.assertTrue(inferred_trip["data"]["inferred_labels"])  # Check for existence here, check for congruence later

    # Tests that each of the (test) algorithms runs and saves to the database correctly
    def testIndividualAlgorithms(self):
        for trip in self.inferred_trips:
            entries = esdt.get_sections_for_trip("inference/labels", self.testUUID, trip.get_id())
            self.assertEqual(len(entries), len(self.test_algorithms))
            for entry in entries:
                self.assertEqual(entry["data"]["trip_id"], trip.get_id())
                this_algorithm = ecwl.AlgorithmTypes(entry["data"]["algorithm_id"])
                self.assertIn(this_algorithm, self.test_algorithms)
                self.assertEqual(entry["data"]["prediction"], self.test_algorithms[this_algorithm](trip))
                self.assertEqual(entry["data"]["start_ts"], trip["data"]["start_ts"])
                self.assertEqual(entry["data"]["end_ts"], trip["data"]["end_ts"])
    
    # Tests that the ensemble algorithm runs and saves to the database correctly
    def testEnsemble(self):
        for trip in self.inferred_trips:
            entries = esdt.get_sections_for_trip("analysis/inferred_labels", self.testUUID, trip.get_id())
            self.assertEqual(len(entries), 1)
            entry = entries[0]
            # TODO: when we have a real ensemble implemented:
            # self.assertEqual(entry["data"]["algorithm_id"], ecwl.AlgorithmTypes.ENSEMBLE)
            # TODO: perhaps assert something about the prediction when we have a real ensemble
            self.assertEqual(entry["data"]["start_ts"], trip["data"]["start_ts"])
            self.assertEqual(entry["data"]["end_ts"], trip["data"]["end_ts"])

    # Tests that the final inferred labels in the inferred trip are the same as those given by the ensemble algorithm
    def testInferredTrip(self):
        for trip in self.inferred_trips:
            entry = esdt.get_sections_for_trip("analysis/inferred_labels", self.testUUID, trip.get_id())[0]
            self.assertEqual(trip["data"]["inferred_labels"], entry["data"]["prediction"])

def main():
    etc.configLogging()
    unittest.main()

if __name__ == "__main__":
    main()
