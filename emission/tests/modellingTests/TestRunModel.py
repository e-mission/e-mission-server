import unittest
import logging

import emission.analysis.modelling.trip_model.model_storage as eamums
import emission.analysis.modelling.trip_model.model_type as eamumt
import emission.analysis.modelling.trip_model.run_model as eamur
import emission.storage.timeseries.abstract_timeseries as esta
import emission.tests.modellingTests.modellingTestAssets as etmm
import emission.storage.decorations.analysis_timeseries_queries as esda
import uuid

class TestRunModel(unittest.TestCase):
    """these tests were copied forward during a refactor of the tour model
    [https://github.com/e-mission/e-mission-server/blob/10772f892385d44e11e51e796b0780d8f6609a2c/emission/analysis/modelling/tour_model_first_only/load_predict.py#L114]

    it's uncertain what condition they are in besides having been refactored to
    use the more recent tour modeling code.    
    """
    
    def setUp(self) -> None:
        """
        sets up the end-to-end run model test with Confirmedtrip data
        """
        logging.basicConfig(format='%(asctime)s:%(levelname)s:%(message)s',
        level=logging.DEBUG)

        # configuration for randomly-generated test data
        self.user_id = user_id = 'TestRunModel-TestData'
        self.origin = (-105.1705977, 39.7402654,)
        self.destination = (-105.1755606, 39.7673075)
        self.min_trips = 14
        total_trips = 100
        clustered_trips = 33    # bins must have at least self.min_trips similar trips by default
        has_label_percent = 0.9 # let's make a few that don't have a label, but invariant
                                # $clustered_trips * $has_label_percent > self.min_trips
                                # must be correct or else this test could fail under some random test cases.

        ts = esta.TimeSeries.get_time_series(user_id)
        test_data = list(ts.find_entries(["analysis/confirmed_trip"]))
        if len(test_data) == 0:
            # generate test data for the database

            logging.debug(f"inserting mock Confirmedtrips into database")

            
            train = etmm.generate_mock_trips(
                user_id=user_id,
                trips=total_trips,
                origin=self.origin,
                destination=self.destination,
                label_data={
                    "mode_labels": ['ebike', 'bike'],
                    "purpose_labels": ['happy-hour', 'dog-park'],
                    "replaced_mode_labels": ['walk']
                },
                within_threshold=clustered_trips,  
                has_label_p=has_label_percent  
                                 # 
            )

            ts.bulk_insert(train)

            # confirm data write did not fail
            test_data = esda.get_entries(key="analysis/confirmed_trip", user_id=user_id, time_query=None)
            if len(test_data) != total_trips:
                logging.debug(f'test invariant failed after generating test data')
                self.fail()
            else:
                logging.debug(f'found {total_trips} trips in database')

    def testRoundTrip(self):
        """
        train a model, save it, load it, and use it for prediction
        """

        logging.debug(f'(TRAIN) creating a model based on trips in database')
        eamur.update_trip_model(
            user_id=self.user_id,
            model_type=eamumt.ModelType.GREEDY_SIMILARITY_BINNING,
            model_storage=eamums.ModelStorage.DOCUMENT_DATABASE,
            min_trips=self.min_trips
        )
        
        logging.debug(f'(TEST) testing prediction of stored model')
        test = etmm.build_mock_trip(
            user_id=self.user_id,
            origin=self.origin,
            destination=self.destination
        )
        prediction, n = eamur.predict_labels_with_n(test)
        logging.debug(prediction)

        self.assertNotEqual(len(prediction), 0, "should have a prediction")
        




    # def setUp(self):
    #     self.all_users = esta.TimeSeries.get_uuid_list()
    #     if len(self.all_users) == 0:
    #         self.fail('test invariant failed: no users found')
    
    # def testTrip1(self):

    #     # case 1: the new trip matches a bin from the 1st round and a cluster from the 2nd round
    #     user_id = self.all_users[0]
    #     eamur.update_trip_model(user_id, eamumt.ModelType.GREEDY_SIMILARITY_BINNING, eamums.ModelStorage.DATABASE)
    #     filter_trips = eamur._get_trips_for_user(user_id, None, 0)
    #     new_trip = filter_trips[4]
    #     # result is [{'labels': {'mode_confirm': 'shared_ride', 'purpose_confirm': 'church', 'replaced_mode': 'drove_alone'},
    #     # 'p': 0.9333333333333333}, {'labels': {'mode_confirm': 'shared_ride', 'purpose_confirm': 'entertainment',
    #     # 'replaced_mode': 'drove_alone'}, 'p': 0.06666666666666667}]
    #     pl, _ = eamur.predict_labels_with_n(new_trip)
    #     assert len(pl) > 0, f"Invalid prediction {pl}"

    # def testTrip2(self):

    #     # case 2: no existing files for the user who has the new trip:
    #     # 1. the user is invalid(< 10 existing fully labeled trips, or < 50% of trips that fully labeled)
    #     # 2. the user doesn't have common trips
    #     user_id = self.all_users[1]
    #     eamur.update_trip_model(user_id, eamumt.ModelType.GREEDY_SIMILARITY_BINNING, eamums.ModelStorage.DATABASE)
    #     filter_trips = eamur._get_trips_for_user(user_id, None, 0)
    #     new_trip = filter_trips[0]
    #     # result is []
    #     pl, _ = eamur.predict_labels_with_n(new_trip)
    #     assert len(pl) == 0, f"Invalid prediction {pl}"

    # def testTrip3(self):

    #     # case3: the new trip is novel trip(doesn't fall in any 1st round bins)
    #     user_id = self.all_users[0]
    #     eamur.update_trip_model(user_id, eamumt.ModelType.GREEDY_SIMILARITY_BINNING, eamums.ModelStorage.DATABASE)
    #     filter_trips = eamur._get_trips_for_user(user_id, None, 0)
    #     new_trip = filter_trips[0]
    #     # result is []
    #     pl = eamur.predict_labels_with_n(new_trip)
    #     assert len(pl) == 0, f"Invalid prediction {pl}"

    # case 4: the new trip falls in a 1st round bin, but predict to be a new cluster in the 2nd round
    # result is []
    # no example for now
