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
import emission.pipeline.intake_stage as epi
import logging
import bson.objectid as boi

import emission.analysis.modelling.trip_model.config as eamtc

import emission.analysis.modelling.trip_model.run_model as eamur
import emission.analysis.modelling.trip_model.model_type as eamumt
import emission.analysis.modelling.trip_model.model_storage as eamums
import emission.tests.modellingTests.modellingTestAssets as etmm
import emission.storage.timeseries.abstract_timeseries as esta


class TestForestModelIntegration(unittest.TestCase):
    # Test if the forest model for label prediction is smoothly integrated with the inference pipeline.
    # In the initial setup, build a dummy forest model. Then run the pipeline on real example data.
    # Finally in the test, assert the type of label predictions expected.

    def setUp(self):
        np.random.seed(91)
        self.test_algorithms = eacilp.primary_algorithms
        forest_model_config = eamtc.get_config_value_or_raise('model_parameters.forest')

        etc.setupRealExample(self, "emission/tests/data/real_examples/shankari_2015-07-22")  ##maybe use a different file
        ts = esta.TimeSeries.get_time_series(self.testUUID)
        label_data = {
            "mode_confirm": ['ebike', 'bike'],
            "purpose_confirm": ['happy-hour', 'dog-park'],
            "replaced_mode": ['walk'],
            "mode_weights": [0.9, 0.1],
            "purpose_weights": [0.1, 0.9]
        }

        self.total_trips=100
        ## generate mock trips
        train = etmm.generate_mock_trips(
            user_id=self.testUUID,
            trips=self.total_trips,
            origin=(-105.1705977, 39.7402654),
            destination=(-105.1755606, 39.7673075),
            trip_part='od',
            label_data=label_data,
            within_threshold= 33,  
            threshold=0.004, # ~400m
            has_label_p=0.9
        )
        ## Required for Forest model inference
        for result_entry in train:
            result_entry['data']['start_local_dt']=result_entry['metadata']['write_local_dt']
            result_entry['data']['end_local_dt']=result_entry['metadata']['write_local_dt']
            result_entry['data']['start_place']=boi.ObjectId()
            result_entry['data']['end_place']=boi.ObjectId()
        ts.bulk_insert(train)
        # confirm data write did not fail
        check_data = esda.get_entries(key="analysis/confirmed_trip", user_id=self.testUUID, time_query=None)
        if len(check_data) != self.total_trips:
            logging.debug(f'test invariant failed after generating test data')
            self.fail()
        else:
            logging.debug(f'found {self.total_trips} trips in database')
        ## Build an already existing model or a new model
        eamur.update_trip_model(
            user_id=self.testUUID,
            model_type=eamumt.ModelType.RANDOM_FOREST_CLASSIFIER,
            model_storage=eamums.ModelStorage.DOCUMENT_DATABASE,
            min_trips=4,
            model_config=forest_model_config
        )
        ## run inference pipeline
        self.run_pipeline(self.test_algorithms)
        time_range = estt.TimeQuery("metadata.write_ts", None, time.time())
        self.inferred_trips = esda.get_entries(esda.INFERRED_TRIP_KEY, self.testUUID, time_query=time_range)

    def tearDown(self):
        self.reset_all()

    def run_pipeline(self, algorithms):
        default_primary_algorithms = eacilp.primary_algorithms
        eacilp.primary_algorithms = algorithms
        epi.run_intake_pipeline_for_user(self.testUUID,skip_if_no_new_data = False)
        eacilp.primary_algorithms = default_primary_algorithms

    def reset_all(self):
        edb.get_analysis_timeseries_db().delete_many({'user_id': self.testUUID})
        edb.get_model_db().delete_many({'user_id': self.testUUID})
        edb.get_pipeline_state_db().delete_many({'user_id': self.testUUID})


    # Tests that forest algorithm being tested runs successfully
    def testForestAlgorithm(self):
        for trip in self.inferred_trips:
            entries = esdt.get_sections_for_trip("inference/labels", self.testUUID, trip.get_id())
            self.assertEqual(len(entries), len(self.test_algorithms))
            for entry in entries:
                self.assertGreater(len(entry["data"]["prediction"]), 0)
                for singleprediction in entry["data"]["prediction"]:
                    self.assertIsInstance(singleprediction, dict, " should be an instance of the dictionary class")
                    self.assertIsInstance(singleprediction['labels'], dict, " should be an instance of the dictionary class")
                    self.assertIn('mode_confirm',singleprediction['labels'].keys())
                    self.assertIn('replaced_mode',singleprediction['labels'].keys())
                    self.assertIn('purpose_confirm',singleprediction['labels'].keys())        

def main():
    etc.configLogging()
    unittest.main()

if __name__ == "__main__":
    main()
