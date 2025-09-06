import unittest
import logging

import emission.analysis.modelling.trip_model.run_model as eamur
import emission.analysis.modelling.trip_model.model_type as eamumt
import emission.analysis.modelling.trip_model.model_storage as eamums
import emission.analysis.modelling.trip_model.models as eamtm
import emission.storage.timeseries.abstract_timeseries as esta
import emission.tests.modellingTests.modellingTestAssets as etmm
import emission.storage.decorations.analysis_timeseries_queries as esda
import emission.core.get_database as edb
import emission.storage.pipeline_queries as epq
import emission.core.wrapper.pipelinestate as ecwp
import emission.analysis.modelling.trip_model.forest_classifier as eamtf
from sklearn.ensemble import RandomForestClassifier 

class TestRunForestModel(unittest.TestCase):
    """
        Tests to ensure Pipeline builds and runs with zero
        or more trips  
    """
    
    def setUp(self):
        """
        sets up the end-to-end run model test with Confirmedtrip data
        """
        logging.basicConfig(format='%(asctime)s:%(levelname)s:%(message)s',
        level=logging.DEBUG)

        # configuration for randomly-generated test data
        self.user_id = user_id = 'TestRunForestModel-TestData'
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
              #values required by forest model
            for entry in train:
                entry['data']['start_local_dt']=entry['metadata']['write_local_dt']
                entry['data']['end_local_dt']=entry['metadata']['write_local_dt']

            ts.bulk_insert(train)

            # confirm data write did not fail
            test_data = esda.get_entries(key="analysis/confirmed_trip", user_id=user_id, time_query=None)
            if len(test_data) != self.total_trips:
                logging.debug(f'test invariant failed after generating test data')
                self.fail()
            else:
                logging.debug(f'found {self.total_trips} trips in database')

    def tearDown(self):
        """
        clean up database
        """
        edb.get_analysis_timeseries_db().delete_many({'user_id': self.user_id})
        edb.get_model_db().delete_many({'user_id': self.user_id})
        edb.get_pipeline_state_db().delete_many({'user_id': self.user_id})

    def testBuildForestModelFromConfig(self):
        """
        forest model takes config arguments via the constructor for testing
        purposes but will load from a file in /conf/analysis/ which is tested here
        """

        built_model = eamumt.ModelType.RANDOM_FOREST_CLASSIFIER.build()
        attributes={'purpose_predictor': RandomForestClassifier ,
                    'mode_predictor' :RandomForestClassifier,
                    'replaced_predictor':RandomForestClassifier,
                    'purpose_enc' : eamtm.OneHotWrapper,
                    'mode_enc':eamtm.OneHotWrapper
                    }
        self.assertIsInstance(built_model,eamtf.ForestClassifierModel)
        for attr in attributes:
            #logging.debug(f'{attr,attributes[attr]}')
            x=getattr(built_model.model,attr)
            self.assertIsInstance(x, attributes[attr])
        # success if it didn't throw

    def testTrainForestModelWithZeroTrips(self):
        """
        forest model takes config arguments via the constructor for testing
        purposes but will load from a file in /conf/analysis/ which is tested here
        """

        # pass along debug model configuration
        forest_model_config= {
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

        logging.debug(f'~~~~ do nothing ~~~~')
        eamur.update_trip_model(
            user_id=self.unused_user_id,
            model_type=eamumt.ModelType.RANDOM_FOREST_CLASSIFIER,
            model_storage=eamums.ModelStorage.DOCUMENT_DATABASE,
            min_trips=self.min_trips,
            model_config=forest_model_config
        )

        # user had no entries so their pipeline state should not have been set
        # if it was set, the time query here would 
        stage = ecwp.PipelineStages.TRIP_MODEL
        pipeline_state = epq.get_current_state(self.unused_user_id, stage)
        self.assertIsNone(
            pipeline_state['curr_run_ts'], 
            "pipeline should not have a current timestamp for the test user")


    def test1RoundPredictForestModel(self):
        """
       forest model takes config arguments via the constructor for testing
       purposes but will load from a file in /conf/analysis/ which is tested here
       """

        forest_model_config= {
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

        logging.debug(f'(TRAIN) creating a model based on trips in database')
        eamur.update_trip_model(
            user_id=self.user_id,
            model_type=eamumt.ModelType.RANDOM_FOREST_CLASSIFIER,
            model_storage=eamums.ModelStorage.DOCUMENT_DATABASE,
            min_trips=self.min_trips,
            model_config=forest_model_config
        )
        
        logging.debug(f'(TEST) testing prediction of stored model')
        test = esda.get_entries(key="analysis/confirmed_trip", user_id=self.user_id, time_query=None)    
        model = eamur._load_stored_trip_model(
            user_id=self.user_id,
            model_type=eamumt.ModelType.RANDOM_FOREST_CLASSIFIER,
            model_storage=eamums.ModelStorage.DOCUMENT_DATABASE,
            model_config=forest_model_config        
            )
             
        predictions_list = eamur.predict_labels_with_n(
            trip_list = test,
            model=model            
        )
        for prediction, n in predictions_list:
            [logging.debug(p) for p in sorted(prediction, key=lambda r: r['p'], reverse=True)]
            self.assertNotEqual(len(prediction), 0, "should have a prediction")
            self.assertIn('labels',prediction[0].keys())
            self.assertIn('p',prediction[0].keys())
            self.assertIsInstance(prediction[0], dict, " should be an instance of the dictionary class")
            self.assertIsInstance(prediction[0]['labels'], dict, " should be an instance of the dictionary class")
            self.assertIn('mode_confirm',prediction[0]['labels'].keys())
            self.assertIn('replaced_mode',prediction[0]['labels'].keys())
            self.assertIn('purpose_confirm',prediction[0]['labels'].keys())