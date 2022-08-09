import unittest
import logging

import emission.analysis.modelling.trip_model.model_storage as eamums
import emission.analysis.modelling.trip_model.model_type as eamumt
import emission.analysis.modelling.trip_model.run_model as eamur
import emission.storage.timeseries.abstract_timeseries as esta
import emission.tests.modellingTests.modellingTestAssets as etmm
import emission.storage.decorations.analysis_timeseries_queries as esda
import emission.core.get_database as edb
import emission.storage.pipeline_queries as epq


class TestRunGreedyModel(unittest.TestCase):
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
        """
        clean up database
        """
        edb.get_analysis_timeseries_db().delete_many({'user_id': self.user_id})

    def testBuildGreedyModelFromConfig(self):
        """
        greedy model takes config arguments via the constructor for testing
        purposes but will load from a file in /conf/analysis/ which is tested here
        """

        eamumt.ModelType.GREEDY_SIMILARITY_BINNING.build()
        # success if it didn't throw

    def testTrainGreedyModelWithZeroTrips(self):
        """
        greedy model takes config arguments via the constructor for testing
        purposes but will load from a file in /conf/analysis/ which is tested here
        """

        # pass along debug model configuration
        greedy_model_config = {
            "metric": "od_similarity",
            "similarity_threshold_meters": 500,
            "apply_cutoff": False,
            "incremental_evaluation": False
        }

        logging.debug(f'~~~~ do nothing ~~~~')
        eamur.update_trip_model(
            user_id=self.unused_user_id,
            model_type=eamumt.ModelType.GREEDY_SIMILARITY_BINNING,
            model_storage=eamums.ModelStorage.DOCUMENT_DATABASE,
            min_trips=self.min_trips,
            model_config=greedy_model_config
        )

        # user had no entries so their pipeline state should not have been set
        # if it was set, the time query here would 
        time_query = epq.get_time_query_for_trip_model(self.unused_user_id)
        self.assertIsNone(time_query, "should not have a pipeline state entry")


    def test1RoundTripGreedySimilarityBinning(self):
        """
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
            "incremental_evaluation": False
        }

        logging.debug(f'(TRAIN) creating a model based on trips in database')
        eamur.update_trip_model(
            user_id=self.user_id,
            model_type=eamumt.ModelType.GREEDY_SIMILARITY_BINNING,
            model_storage=eamums.ModelStorage.DOCUMENT_DATABASE,
            min_trips=self.min_trips,
            model_config=greedy_model_config
        )
        
        logging.debug(f'(TEST) testing prediction of stored model')
        test = etmm.build_mock_trip(
            user_id=self.user_id,
            origin=self.origin,
            destination=self.destination
        )
        prediction, n = eamur.predict_labels_with_n(
            trip = test,
            model_type=eamumt.ModelType.GREEDY_SIMILARITY_BINNING,
            model_storage=eamums.ModelStorage.DOCUMENT_DATABASE,
            model_config=greedy_model_config
        )

        [logging.debug(p) for p in sorted(prediction, key=lambda r: r['p'], reverse=True)]

        self.assertNotEqual(len(prediction), 0, "should have a prediction")

