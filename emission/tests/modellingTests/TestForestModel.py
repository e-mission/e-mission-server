import unittest
import logging
import numpy as np
import uuid
import json
import os

import emission.analysis.modelling.trip_model.run_model as eamur
import emission.analysis.modelling.trip_model.model_type as eamumt
import emission.analysis.modelling.trip_model.model_storage as eamums
import emission.storage.json_wrappers as esj
import emission.storage.timeseries.abstract_timeseries as esta
import emission.tests.modellingTests.modellingTestAssets as etmm
import emission.storage.decorations.analysis_timeseries_queries as esda
import emission.core.get_database as edb
import emission.core.wrapper.entry as ecwe
import emission.storage.decorations.analysis_timeseries_queries as esdatq

class TestForestModel(unittest.TestCase):

    def setUp(self):
        """
        sets up the end-to-end run model test with Confirmedtrip data
        """
        logging.basicConfig(format='%(asctime)s:%(levelname)s:%(message)s',
        level=logging.DEBUG)

        self.user_id = uuid.UUID('aa9fdec9-2944-446c-8ee2-50d79b3044d3')
        self.ts = esta.TimeSeries.get_time_series(self.user_id)
        self.new_trips_per_invocation = 3
        self.model_type = eamumt.ModelType.RANDOM_FOREST_CLASSIFIER
        self.model_storage = eamums.ModelStorage.DOCUMENT_DATABASE
        sim_threshold = 500  # meters
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

        existing_entries_for_user = list(self.ts.find_entries([esdatq.CONFIRMED_TRIP_KEY]))  
        if len(existing_entries_for_user) != 0:
            raise Exception(f"test invariant failed, there should be no entries for user {self.user_id}")
        
        # load in trips from a test file source
        input_file = 'emission/tests/data/real_examples/shankari_2016-06-20.expected_confirmed_trips'
        with open(input_file, 'r') as f:
            trips_json = json.load(f, object_hook=esj.wrapped_object_hook)
            self.trips = [ecwe.Entry(r) for r in trips_json]
        logging.debug(f'loaded {len(self.trips)} trips from {input_file}')
   
    def tearDown(self):
        """
        clean up database
        """
        edb.get_analysis_timeseries_db().delete_many({'user_id': self.user_id})
        edb.get_model_db().delete_many({'user_id': self.user_id})
        edb.get_pipeline_state_db().delete_many({'user_id': self.user_id})



    def testRandomForestRegression(self):
        """
        test to ensure consistent model results. Load data for a user from json, split
        into train and test. After training, we generate predictions and match them with
        predictions from last time. If the code is run for the first time, the current predicitons
        will be stored as ground truth.
        """
        file_path= 'emission/tests/modellingTests/data.json'
        split=int(0.9*len(self.trips))
        train_data= self.trips[:split]

        self.ts.bulk_insert(train_data)

        # confirm write to database succeeded
        self.initial_data = list(self.ts.find_entries([esdatq.CONFIRMED_TRIP_KEY]))
        if len(self.initial_data) == 0:
            logging.debug(f'Writing train data failed')
            self.fail()

        test_data=self.trips[split:]
        logging.debug(f'LENDATA{len(train_data),len(test_data)}')
        eamur.update_trip_model(
            user_id=self.user_id,
            model_type=eamumt.ModelType.RANDOM_FOREST_CLASSIFIER,
            model_storage=eamums.ModelStorage.DOCUMENT_DATABASE,
            min_trips=4,
            model_config=self.forest_model_config
        )
        model = eamur._load_stored_trip_model(
            user_id=self.user_id,
            model_type=eamumt.ModelType.RANDOM_FOREST_CLASSIFIER,
            model_storage=eamums.ModelStorage.DOCUMENT_DATABASE,
            model_config=self.forest_model_config        
            )
        
        curr_predictions_list = eamur.predict_labels_with_n(
            trip_list = [test_data],
            model=model           
        )


        ## predictions take the form like :
        #
        #{'labels': {'mode_confirm': 'ebike', 'replaced_mode': 'walk', 'purpose_confirm': 'dog-park'}, 'p': 1.0}       

        #Below are two ways we can store prev. predictions  list . Whichever way we finalise, I'll delete the other one.
        #
        #Method 1 : Run predictions for the first time and hardcode them into
        #prev_prdictions_list. For every iteration, simply  compare them
        #
        # for the current data that we read from json, the predictions we get is an empty list. If 
        # we use a different file with more data, this'll take the for as mentioned above
        #
        prev_predictions_list= [
            (
                [],
                -1
            )
        ]   

        self.assertEqual(prev_predictions_list,curr_predictions_list," previous predictions should match current predictions")


        #Method 2 ( which was failing): Store these predictions into a json and read from
        #that json
        # 
        # try:
        #     if os.path.exists(file_path) and os.path.getsize(file_path)>0:
        #         with open(file_path, 'r') as f:
        #             prev_predictions_list = json.load(f)
        #             logging.debug()
        #             self.assertEqual(prev_predictions_list,curr_predictions_list," previous predictions should match current predictions")
        #     else:
        #         with open(file_path,'w') as file:
        #             json.dump(curr_predictions_list,file,indent=4)
        #             logging.debug("Previous predicitons stored for future matching" )
        # except json.JSONDecodeError:
        #     logging.debug("jsonDecodeErrorError")