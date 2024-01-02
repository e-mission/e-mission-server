# This tests the label inference pipeline. It uses real  data and placeholder inference algorithms
import unittest
import numpy as np
import time
import emission.analysis.classification.inference.labels.pipeline as eacilp
import emission.analysis.classification.inference.labels.inferrers as eacili
import emission.core.wrapper.labelprediction as ecwl
import emission.storage.decorations.analysis_timeseries_queries as esda
import emission.storage.decorations.trip_queries as esdt
import emission.storage.timeseries.timequery as estt
import emission.core.get_database as edb
import emission.tests.common as etc
import logging

class TestLabelInferencePipeline(unittest.TestCase):
    # It is important that these functions be deterministic
    

    def setUp(self):

        self.reset_all()
        np.random.seed(91)
        self.test_algorithms = eacilp.primary_algorithms
        etc.setupRealExample(self, "emission/tests/data/real_examples/shankari_2015-07-22")  ##maybe use a different file
        self.run_pipeline(self.test_algorithms)
        time_range = estt.TimeQuery("metadata.write_ts", None, time.time())
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

    # Tests that each of the (test) algorithms runs and saves to the database correctly
    def testIndividualAlgorithms(self):
        for trip in self.inferred_trips:
            entries = esdt.get_sections_for_trip("inference/labels", self.testUUID, trip.get_id())
            self.assertEqual(len(entries), len(self.test_algorithms))
            for entry in entries:
                self.assertGreater(len(entry["data"]["prediction"]), 0)


def main():
    etc.configLogging()
    unittest.main()

if __name__ == "__main__":
    main()
