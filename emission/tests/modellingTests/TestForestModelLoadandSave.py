from typing import ByteString
import unittest
import logging
import unittest.mock as um
import emission.analysis.modelling.trip_model.run_model as eamur
import emission.analysis.modelling.trip_model.model_type as eamumt
import emission.analysis.modelling.trip_model.model_storage as eamums
import emission.analysis.modelling.trip_model.config as eamtc
import uuid
import emission.storage.timeseries.abstract_timeseries as esta
import emission.tests.modellingTests.modellingTestAssets as etmm
import emission.storage.decorations.analysis_timeseries_queries as esda
import emission.core.get_database as edb
import emission.analysis.modelling.trip_model.run_model as eamtr
import emission.tests.common as etc

class TestForestModelLoadandSave(unittest.TestCase):
    """
    Tests to make sure the model load and save properly
    The label_data dict and mock_trip_data are copied over from TestRunGreedyModel.py
    """
    def setUp(self): 
        self.user_id = 'TestForestModelLoadAndSave-TestData'
        self.unused_user_id = 'asdjfkl;asdfjkl;asd08234ur13fi4jhf2103mkl'
        ts = esta.TimeSeries.get_time_series(self.user_id)

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
            user_id=self.user_id,
            trips=100,
            origin=(-105.1705977, 39.7402654,),
            destination=(-105.1755606, 39.7673075),
            trip_part='od',
            label_data=label_data,
            within_threshold=33,  
            threshold=0.004, # ~400m
            has_label_p=0.9
        )

        # Required for Forest model inference
        for result_entry in mock_trip_data:
            result_entry['data']['start_local_dt']=result_entry['metadata']['write_local_dt']
            result_entry['data']['end_local_dt']=result_entry['metadata']['write_local_dt']

        split = int(len(mock_trip_data)*0.7)  
        mock_train_data = mock_trip_data[:split]
        self.mock_test_data = mock_trip_data[split:]

        ts.bulk_insert(mock_train_data)

        self.forest_model_config= eamtc.get_config_value_or_raise('model_parameters.forest')

        # Build and train model
        logging.debug(f'(TRAIN) creating a model based on trips in database')
        eamur.update_trip_model(
            user_id=self.user_id,
            model_type=eamumt.ModelType.RANDOM_FOREST_CLASSIFIER,
            model_storage=eamums.ModelStorage.DOCUMENT_DATABASE,
            min_trips=14,
            model_config=self.forest_model_config
        )

        self.model = eamur._load_stored_trip_model(
            user_id=self.user_id,
            model_type=eamumt.ModelType.RANDOM_FOREST_CLASSIFIER,
            model_storage=eamums.ModelStorage.DOCUMENT_DATABASE,
            model_config=self.forest_model_config        
        )
       
    def tearDown(self):  
        etc.dropAllCollections(edb._get_current_db())

    def testForestModelPredictionsEquality(self):
        """
        EqualityTest : Serialising an object with 'to_dict' and then immediately 
        deserialize it with 'from_dict'. After deserialization, the object should have 
        the same state as original 

        TypePreservationTest: To ensure that the serialization and deserialization
        process maintains the data types of all model attributes. 
        The type of deserialized model attributes and the predictions of this must match 
        those of initial model.
        """
        predictions_list = eamur.predict_labels_with_n(
            trip_list = self.mock_test_data,
            model=self.model
        )

        model_data=self.model.to_dict()
        deserialized_model_type=eamumt.ModelType.RANDOM_FOREST_CLASSIFIER
        deserialized_model = deserialized_model_type.build(self.forest_model_config)
        deserialized_model.from_dict(model_data)

        predictions_deserialized_model_list = eamur.predict_labels_with_n(
                trip_list = self.mock_test_data,
                model=deserialized_model
        )

        # Test if the types are correct
        for attr in ['purpose_predictor','mode_predictor','replaced_predictor','purpose_enc','mode_enc','train_df']:
            deserialized_attr_value=getattr(deserialized_model.model,attr)
            original_attr_value=getattr(self.model.model,attr)
            # Check type preservation
            self.assertIsInstance(deserialized_attr_value,type(original_attr_value), f"Type mismatch for {attr} ")

        # Test if the values are the same
        self.assertEqual(predictions_list, predictions_deserialized_model_list, " should be equal")

    def testForestModelConsistency(self):
        """
        ConsistencyTest : To Verify that the serialization and deserialization process
        is consistent across multiple executions
        """
        predictions_list_model1 = eamur.predict_labels_with_n(
            trip_list = self.mock_test_data,
            model=self.model           
        )

        model_iter2 = eamur._load_stored_trip_model(
            user_id=self.user_id,
            model_type=eamumt.ModelType.RANDOM_FOREST_CLASSIFIER,
            model_storage=eamums.ModelStorage.DOCUMENT_DATABASE,
            model_config=self.forest_model_config        
        )

        predictions_list_model2 = eamur.predict_labels_with_n(
            trip_list = self.mock_test_data,
            model=model_iter2           
        )

        self.assertEqual(predictions_list_model1, predictions_list_model2, " should be equal")

    def testSerializationDeserializationErrorHandling(self):
        """
        SerializationErrorHandling : To verify that any errors during
        serialising an object with 'to_dict' are handled.

        DeserializationErrorHandling : To verify that any errors during
        deserialising an object with 'from_dict' are handled.
        """
        # Test 1: SerializationErrorHandling
        # Defining a side effect function to simulate a serialization error
        def mock_dump(*args,**kwargs):
            raise Exception("Serialization Error")

        # patch is used to temporarily replace joblib.dump with a 
        # mock function that raises an exception
        #
        # side_effect, which is set to mock_dump, is called instead of
        # real joblib.dump function when 'to_dict' is invoked
        with um.patch('joblib.dump',side_effect=mock_dump):
            with self.assertRaises(RuntimeError):
                self.model.to_dict()

        # Test 2: DeserializationErrorHandling 
        # Defining a side effect function to simulate a deserialization error
        def mock_load(*args,**kwargs):
            raise Exception("Deserialization Error")

        model_data=self.model.to_dict()
        deserialized_model_type=eamumt.ModelType.RANDOM_FOREST_CLASSIFIER
        deserialized_model = deserialized_model_type.build(self.forest_model_config)

        # patch is used to temporarily replace joblib.load with a 
        # mock function that raises an exception
        #
        # side_effect, which is set to mock_load, is called instead of
        # real joblib.load function when 'to_dict' is invoked
        with um.patch('joblib.load',side_effect=mock_load):
            with self.assertRaises(RuntimeError):
                deserialized_model.from_dict(model_data)

if __name__ == '__main__':
    etc.configLogging()
    unittest.main()
