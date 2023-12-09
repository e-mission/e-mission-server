from typing import ByteString
import unittest
import logging
import pytest
from unittest.mock import patch
import emission.analysis.modelling.trip_model.run_model as eamur
import emission.analysis.modelling.trip_model.model_type as eamumt
import emission.analysis.modelling.trip_model.model_storage as eamums

import emission.storage.timeseries.abstract_timeseries as esta
import emission.tests.modellingTests.modellingTestAssets as etmm
import emission.storage.decorations.analysis_timeseries_queries as esda
import emission.core.get_database as edb
import emission.storage.pipeline_queries as epq
import emission.core.wrapper.pipelinestate as ecwp


class TestForestModelLoadandSave(unittest.TestCase):
    """these tests were copied forward during a refactor of the tour model
    [https://github.com/e-mission/e-mission-server/blob/10772f892385d44e11e51e796b0780d8f6609a2c/emission/analysis/modelling/tour_model_first_only/load_predict.py#L114]

    it's uncertain what condition they are in besides having been refactored to
    use the more recent tour modeling code.    
    """
    
    def setUp(self):
        """
        sets up the end-to-end run model test with Confirmedtrip data
        """
        logging.basicConfig(format='%(asctime)s:%(levelname)s:%(message)s',
        level=logging.DEBUG)

        # configuration for randomly-generated test data
        self.user_id = user_id = 'TestForestModelLoadAndSave-TestData'
        self.origin = (-105.1705977, 39.7402654,)
        self.destination = (-105.1755606, 39.7673075)
        self.min_trips = 14
        self.total_trips = 100
        self.clustered_trips = 33    # must have at least self.min_trips similar trips by default
        self.has_label_percent = 0.9 # let's make a few that don't have a label, but invariant
                                # $clustered_trips * $has_label_percent > self.min_trips
                                # must be correct or else this test could fail under some random test cases.

        # for a negative test, below
        self.unused_user_id = 'asdjfkl;asdfjkl;asd08234ur13fi4jhf2103mkl'

        # test data can be saved between test invocations, check if data exists before generating
        ts = esta.TimeSeries.get_time_series(user_id)
        test_data = list(ts.find_entries(["analysis/confirmed_trip"]))  
        if len(test_data) == 0:
            # generate test data for the database
            logging.debug(f"inserting mock Confirmedtrips into database")
            
            # generate labels with a known sample weight that we can rely on in the test
            label_data = {
                "mode_confirm": ['ebike', 'bike'],
                "purpose_confirm": ['happy-hour', 'dog-park'],
                "replaced_mode": ['walk'],
                "mode_weights": [0.9, 0.1],
                "purpose_weights": [0.1, 0.9]
            }

            train = etmm.generate_mock_trips(
                user_id=user_id,
                trips=self.total_trips,
                origin=self.origin,
                destination=self.destination,
                trip_part='od',
                label_data=label_data,
                within_threshold=self.clustered_trips,  
                threshold=0.004, # ~400m
                has_label_p=self.has_label_percent
            )

            ts.bulk_insert(train)

            # confirm data write did not fail
            test_data = esda.get_entries(key="analysis/confirmed_trip", user_id=user_id, time_query=None)
            if len(test_data) != self.total_trips:
                logging.debug(f'test invariant failed after generating test data')
                self.fail()
            else:
                logging.debug(f'found {self.total_trips} trips in database')

            self.forest_model_config= {
            "loc_feature" : "coordinates",
            "radius": 500,
            "size_thresh":1,
            "purity_thresh":1.0,
            "gamma":0.05,
            "C":1,
            "n_estimators":100,
            "criterion":"gini",
            "max_depth":'null',
            "min_samples_split":2,
            "min_samples_leaf":1,
            "max_features":"sqrt",
            "bootstrap":True,
            "random_state":42,
            "use_start_clusters":False,
            "use_trip_clusters":True
        }
                
    def tearDown(self):
        """
        clean up database
        """
        edb.get_analysis_timeseries_db().delete_many({'user_id': self.user_id})
        edb.get_model_db().delete_many({'user_id': self.user_id})
        edb.get_pipeline_state_db().delete_many({'user_id': self.user_id})

    def testForestModelRoundTrip(self):
        """
        RoundTripTest : Serialising an object with 'to_dict' and then immediately 
        deserialize it with 'from_dict'. After deserialization, the object should have 
        the same state as original 
        """

#        logging.debug(f'creating Random Forest model based on trips in database')
        eamur.update_trip_model(
            user_id=self.user_id,
            model_type=eamumt.ModelType.RANDOM_FOREST_CLASSIFIER,
            model_storage=eamums.ModelStorage.DOCUMENT_DATABASE,
            min_trips=self.min_trips,
            model_config=self.forest_model_config
        )
        
        model = eamur._load_stored_trip_model(
            user_id=self.user_id,
            model_type=eamumt.ModelType.RANDOM_FOREST_CLASSIFIER,
            model_storage=eamums.ModelStorage.DOCUMENT_DATABASE,
            model_config=self.forest_model_config        
            )

#        logging.debug(f'Loading test data')
        test = esda.get_entries(key="analysis/confirmed_trip", user_id=self.user_id, time_query=None)    

#       logging.debug(f'Predictions on trips in database')

        predictions_list = eamur.predict_labels_with_n(
            trip_list = [test],
            model=model            
        )

 #       logging.debug(f'Serialising the model ')

        model_data=model.to_dict()

#        logging.debug(f'Deserialising the model')


        deserialized_model_type=eamumt.ModelType.RANDOM_FOREST_CLASSIFIER
        deserialized_model = deserialized_model_type.build(self.forest_model_config)
        deserialized_model.from_dict(model_data)

#       logging.debug(f'Predictions on trips using deserialised model')
        predictions_loaded_model_list = eamur.predict_labels_with_n(
                trip_list = [test],
                model=deserialized_model           
        )
#       logging.debug(f'Assert that both predictions are the same')
        self.assertEqual(predictions_list, predictions_loaded_model_list, " should be equal")

    def testForestModelConsistency(self):
        """
        ConsistencyTest : To Verify that the serialization and deserialization process
        is consistent across multiple executions
        """
    #    logging.debug(f'creating a model based on trips in database')
        eamur.update_trip_model(
            user_id=self.user_id,
            model_type=eamumt.ModelType.RANDOM_FOREST_CLASSIFIER,
            model_storage=eamums.ModelStorage.DOCUMENT_DATABASE,
            min_trips=self.min_trips,
            model_config=self.forest_model_config
        )

        model_iter1 = eamur._load_stored_trip_model(
            user_id=self.user_id,
            model_type=eamumt.ModelType.RANDOM_FOREST_CLASSIFIER,
            model_storage=eamums.ModelStorage.DOCUMENT_DATABASE,
            model_config=self.forest_model_config        
            )
        
        # logging.debug(f'Load Test data')
        test = esda.get_entries(key="analysis/confirmed_trip", user_id=self.user_id, time_query=None)    
       
        # logging.debug(f' Model Predictions on trips in database')

        predictions_list_model1 = eamur.predict_labels_with_n(
            trip_list = [test],
            model=model_iter1           
        )
        # logging.debug(f' Loading Model again')

        model_iter2 = eamur._load_stored_trip_model(
            user_id=self.user_id,
            model_type=eamumt.ModelType.RANDOM_FOREST_CLASSIFIER,
            model_storage=eamums.ModelStorage.DOCUMENT_DATABASE,
            model_config=self.forest_model_config        
            )
        # logging.debug(f' Model Predictions on trips in database')
        predictions_list_model2 = eamur.predict_labels_with_n(
            trip_list = [test],
            model=model_iter2           
        )
        
        self.assertEqual(predictions_list_model1, predictions_list_model2, " should be equal")



    def testSerializationErrorHandling(self):
        """
        SerialisationErrorHandling : To verify that any errors during
        serialising an object with 'to_dict' are handled.
        """
        # defining a side effect function to simulate a serialization error
        def mock_dump(*args,**kwargs):
            raise Exception("Serialization Error")

        logging.debug(f'(TRAIN) creating a model based on trips in database')
        eamur.update_trip_model(
            user_id=self.user_id,
            model_type=eamumt.ModelType.RANDOM_FOREST_CLASSIFIER,
            model_storage=eamums.ModelStorage.DOCUMENT_DATABASE,
            min_trips=self.min_trips,
            model_config=self.forest_model_config
        )
             
        model = eamur._load_stored_trip_model(
            user_id=self.user_id,
            model_type=eamumt.ModelType.RANDOM_FOREST_CLASSIFIER,
            model_storage=eamums.ModelStorage.DOCUMENT_DATABASE,
            model_config=self.forest_model_config        
            )
        # patch is used to temporarily replace joblib.dump with a 
        # mock function that raises an exception
        #
        # side_effect, which is set to mock_dump, is called instead of
        # real joblib.dump function when 'to_dict' is invoked

        with patch('joblib.dump',side_effect=mock_dump):
            with self.assertRaises(RuntimeError):
                model.to_dict()


    def testDeserializationErrorHandling(self):
        """
        deserialisationErrorHandling : To verify that any errors during
        deserialising an object with 'from_dict' are handled.
        """
    # defining a side effect function to simulate a deserialization error
        def mock_load(*args,**kwargs):
            raise Exception("Deserialization Error")
        
        logging.debug(f'(TRAIN) creating a model based on trips in database')
        eamur.update_trip_model(
            user_id=self.user_id,
            model_type=eamumt.ModelType.RANDOM_FOREST_CLASSIFIER,
            model_storage=eamums.ModelStorage.DOCUMENT_DATABASE,
            min_trips=self.min_trips,
            model_config=self.forest_model_config
        )
             
        model = eamur._load_stored_trip_model(
            user_id=self.user_id,
            model_type=eamumt.ModelType.RANDOM_FOREST_CLASSIFIER,
            model_storage=eamums.ModelStorage.DOCUMENT_DATABASE,
            model_config=self.forest_model_config        
            )
      
        model_data=model.to_dict()

        deserialized_model_type=eamumt.ModelType.RANDOM_FOREST_CLASSIFIER
        deserialized_model = deserialized_model_type.build(self.forest_model_config)
        # patch is used to temporarily replace joblib.load with a 
        # mock function that raises an exception
        #
        # side_effect, which is set to mock_load, is called instead of
        # real joblib.load function when 'to_dict' is invoked

        with patch('joblib.load',side_effect=mock_load):
            with self.assertRaises(RuntimeError):
                deserialized_model.from_dict(model_data)


    def testRandomForestTypePreservation(self):
        """
         TypePreservationTest: To ensure that the serialization and deserialization
         process maintains the data types of all model attributes.
        """

        logging.debug(f'(TRAIN) creating a model based on trips in database')
        eamur.update_trip_model(
            user_id=self.user_id,
            model_type=eamumt.ModelType.RANDOM_FOREST_CLASSIFIER,
            model_storage=eamums.ModelStorage.DOCUMENT_DATABASE,
            min_trips=self.min_trips,
            model_config=self.forest_model_config
        )
             
        model = eamur._load_stored_trip_model(
            user_id=self.user_id,
            model_type=eamumt.ModelType.RANDOM_FOREST_CLASSIFIER,
            model_storage=eamums.ModelStorage.DOCUMENT_DATABASE,
            model_config=self.forest_model_config        
            )
                
        model_data=model.to_dict()
        loaded_model_type=eamumt.ModelType.RANDOM_FOREST_CLASSIFIER
        loaded_model = loaded_model_type.build(self.forest_model_config)
        loaded_model.from_dict(model_data)

        
        for attr in ['purpose_predictor','mode_predictor','replaced_predictor','purpose_enc','mode_enc','train_df']:
            assert isinstance(getattr(loaded_model.model,attr),type(getattr(model.model,attr)))

