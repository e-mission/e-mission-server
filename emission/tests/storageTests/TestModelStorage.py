from __future__ import unicode_literals
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import
# Standard imports
from future import standard_library
standard_library.install_aliases()
from builtins import *
import unittest
import datetime as pydt
import logging
import json
import pymongo
import uuid

# Our imports
import emission.core.get_database as edb
import emission.storage.decorations.analysis_timeseries_queries as esda
import emission.analysis.modelling.trip_model.model_storage as eamums
import emission.analysis.modelling.trip_model.model_type as eamumt
import emission.analysis.modelling.trip_model.run_model as eamur
import emission.storage.timeseries.abstract_timeseries as esta
import emission.tests.modellingTests.modellingTestAssets as etmm
import emission.analysis.modelling.trip_model.config as eamtc

# Test imports
import emission.tests.common as etc

class TestModelStorage(unittest.TestCase):
    '''
        Copied over the below code in setup() and testTrimModelEntries() 
        for model creation using mock dummy trips data from 
        emission.tests.modellingTests.TestRunGreedyModel.py
    '''
    def setUp(self):
        logging.basicConfig(format='%(asctime)s:%(levelname)s:%(message)s',
        level=logging.DEBUG)

        # configuration for randomly-generated test data
        self.user_id = user_id = 'TestRunGreedyModel-TestData'
        self.origin = (-105.1705977, 39.7402654,)
        self.destination = (-105.1755606, 39.7673075)
        self.min_trips = 14
        self.total_trips = 100
        self.clustered_trips = 33    # bins must have at least self.min_trips similar trips by default
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

    def tearDown(self):
        edb.get_analysis_timeseries_db().delete_many({'user_id': self.user_id})
        edb.get_model_db().delete_many({'user_id': self.user_id})
        edb.get_pipeline_state_db().delete_many({'user_id': self.user_id})
        
    def testTrimModelEntries(self):
        """
        Took this code from emission.tests.modellingTests.TestRunGreedyModel.py
        with the objective of inserting multiple models into the model_db.
        The test involves building and inserting 20 models, which is greater than 
        the maximum_stored_model_count (= 3) limit defined in conf/analysis/trip_model.conf.json.sample

        train a model, save it, load it, and use it for prediction, using
        the high-level training/testing API provided via 
        run_model.py:update_trip_model()     # train
        run_model.py:predict_labels_with_n() # test

        for clustering, use the default greedy similarity binning model
        """
        # pass along debug model configuration
        greedy_model_config = {
            "metric": "od_similarity",
            "similarity_threshold_meters": 500,
            "apply_cutoff": False,
            "clustering_way": 'origin-destination',
            "incremental_evaluation": False
        }
        maximum_stored_model_count = eamtc.get_maximum_stored_model_count()
        logging.debug(f'(TRAIN) creating a model based on trips in database')
        for i in range(20):
            logging.debug(f"Creating dummy model no. {i}")
            eamur.update_trip_model(
                user_id=self.user_id,
                model_type=eamumt.ModelType.GREEDY_SIMILARITY_BINNING,
                model_storage=eamums.ModelStorage.DOCUMENT_DATABASE,
                min_trips=self.min_trips,
                model_config=greedy_model_config
            )
            current_model_count = edb.get_model_db().count_documents({"user_id": self.user_id})
            if i <= (maximum_stored_model_count - 1):
                self.assertEqual(current_model_count, i+1)
            else:
                self.assertEqual(current_model_count, maximum_stored_model_count)

if __name__ == '__main__':
    import emission.tests.common as etc
    etc.configLogging()
    unittest.main()
