import unittest
import numpy as np
import time
import logging
import bson.objectid as boi
import emission.analysis.classification.inference.labels.pipeline as eacilp
import emission.analysis.classification.inference.labels.inferrers as eacili
import emission.core.wrapper.labelprediction as ecwl
import emission.storage.decorations.analysis_timeseries_queries as esda
import emission.storage.decorations.trip_queries as esdt
import emission.storage.timeseries.timequery as estt
import emission.core.get_database as edb
import emission.tests.common as etc
import emission.pipeline.intake_stage as epi
import emission.analysis.modelling.trip_model.config as eamtc
import emission.analysis.modelling.trip_model.run_model as eamur
import emission.analysis.modelling.trip_model.model_type as eamumt
import emission.analysis.modelling.trip_model.model_storage as eamums
import emission.tests.modellingTests.modellingTestAssets as etmm
import emission.storage.timeseries.abstract_timeseries as esta

class TestForestModelIntegration(unittest.TestCase):
    """
    This tests the label inference pipeline. It uses real data and placeholder inference algorithms.
    Test if the forest model for label prediction is smoothly integrated with the inference pipeline.
    In the initial setup, build a dummy forest model. Then run the pipeline on real example data.
    Finally in the test, assert the type of label predictions expected.
    The label_data dict and mock_trip_data are copied over from TestRunGreedyModel.py
    """
    def setUp(self):
        np.random.seed(91)
        self.test_algorithms = eacilp.primary_algorithms
        forest_model_config = eamtc.get_config_value_or_raise('model_parameters.forest')
        etc.setupRealExample(self, "emission/tests/data/real_examples/shankari_2015-07-22")  ##maybe use a different file
        ts = esta.TimeSeries.get_time_series(self.testUUID)

        # Generate labels with a known sample weight that we can rely on in the test
        label_data = {
            "mode_confirm": ['ebike', 'bike'],
            "purpose_confirm": ['happy-hour', 'dog-park'],
            "replaced_mode": ['walk'],
            "mode_weights": [0.9, 0.1],
            "purpose_weights": [0.1, 0.9]
        }

        # Configuration values for randomly-generated test data copied over from TestRunGreedyModel.py
        mock_trip_data = etmm.generate_mock_trips(
            user_id=self.testUUID,
            trips=100,
            origin=(-105.1705977, 39.7402654),
            destination=(-105.1755606, 39.7673075),
            trip_part='od',
            label_data=label_data,
            within_threshold= 33,  
            threshold=0.004, # ~400m
            has_label_p=0.9
        )

        # Required for Forest model inference
        for result_entry in mock_trip_data:
            result_entry['data']['start_local_dt']=result_entry['metadata']['write_local_dt']
            result_entry['data']['end_local_dt']=result_entry['metadata']['write_local_dt']
            result_entry['data']['start_place']=boi.ObjectId()
            result_entry['data']['end_place']=boi.ObjectId()

        split = int(len(mock_trip_data)*0.7)
        mock_train_data = mock_trip_data[:split]
        self.mock_test_data = mock_trip_data[split:]

        ts.bulk_insert(mock_train_data)

        # Build and train model
        logging.debug(f'(TRAIN) creating a model based on trips in database')
        eamur.update_trip_model(
            user_id=self.testUUID,
            model_type=eamumt.ModelType.RANDOM_FOREST_CLASSIFIER,
            model_storage=eamums.ModelStorage.DOCUMENT_DATABASE,
            min_trips=14,
            model_config=forest_model_config
        )

        # Run inference pipeline
        self.run_pipeline(self.test_algorithms)
        time_range = estt.TimeQuery("metadata.write_ts", None, time.time())
        self.inferred_trips = esda.get_entries(esda.INFERRED_TRIP_KEY, self.testUUID, time_query=time_range)

    def tearDown(self):
        etc.dropAllCollections(edb._get_current_db())

    def run_pipeline(self, algorithms):
        default_primary_algorithms = eacilp.primary_algorithms
        eacilp.primary_algorithms = algorithms
        epi.run_intake_pipeline_for_user(self.testUUID,skip_if_no_new_data = False)
        eacilp.primary_algorithms = default_primary_algorithms

    def testForestAlgorithm(self):
        '''
        Tests that forest algorithm runs successfully when called from the analysis pipeline
        The tests are based on the existing tests in TestLabelInferencePipeline.py
        '''
        valid_modes = ['ebike', 'bike']
        valid_purposes = ['happy-hour', 'dog-park']

        for trip in self.inferred_trips:
            entries = esdt.get_sections_for_trip("inference/labels", self.testUUID, trip.get_id())
            self.assertEqual(len(entries), len(self.test_algorithms))
            for entry in entries:
                # Test 1: Check that non-empty prediction list is generated
                self.assertGreater(len(entry["data"]["prediction"]), 0, "Prediction list should not be empty - model failed to generate any predictions")

                # Test 2: Check for equality of trip inferred labels and prediction value in entry
                self.assertEqual(trip["data"]["inferred_labels"], entry["data"]["prediction"])

                # Test 3: Check that prediction value in entry is equal to the prediction generated by the algorithm
                this_algorithm = ecwl.AlgorithmTypes(entry["data"]["algorithm_id"])
                self.assertIn(this_algorithm, self.test_algorithms)
                self.assertEqual(entry["data"]["prediction"], self.test_algorithms[this_algorithm]([trip])[0])

                for singleprediction in entry["data"]["prediction"]:
                    # Test 4: Check that the prediction is a dictionary
                    self.assertIsInstance(singleprediction, dict, "should be an instance of the dictionary class")
                    self.assertIsInstance(singleprediction['labels'], dict, "should be an instance of the dictionary class")

                    # Test 5: Check that the prediction dictionary contains the required keys
                    self.assertIn('mode_confirm', singleprediction['labels'].keys())
                    self.assertIn('replaced_mode', singleprediction['labels'].keys())
                    self.assertIn('purpose_confirm', singleprediction['labels'].keys())  
                    
                    # Test 6: Check that the prediction dictionary contains the correct values
                    self.assertIn(singleprediction['labels']['mode_confirm'], valid_modes)
                    self.assertIn(singleprediction['labels']['purpose_confirm'], valid_purposes)

def main():
    etc.configLogging()
    unittest.main()

if __name__ == "__main__":
    main()
