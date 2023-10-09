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
import emission.core.wrapper.pipelinestate as ecwp
import numpy as np

class TestRunForestModel(unittest.TestCase):
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


#     def test_model_consistency(self):
#         """
#         Test to ensure that the model's predictions on the mock data remain consistent.
#         """
#         # Get the mock data from the parent class's setup
#         mock_data = self.mock_data

#         # Predict using the model
#         current_predictions = eamur.predict_labels_with_n(
#             trip=mock_data, 
#             model_type=eamumt.ModelType.RANDOM_FOREST_CLASSIFIER, 
#             model_storage=eamums.ModelStorage.DOCUMENT_DATABASE
#         )  # assuming this is how you get predictions
#   ## TODO : 
#         # Check if there are any previously stored predictions
#         stored_predictions = list(self.collection.find({}))  

#         if len(stored_predictions) == 0:
#             # If not, store the current predictions as the ground truth
#             self.collection.insert_many([{"index": i, "prediction": p} for i, p in enumerate(current_predictions)])
#             logging.debug("Stored current model predictions as ground truth.")
#         else:
#             # If there are stored predictions, compare them with the current predictions
#             for stored_pred in stored_predictions:
#                 index, stored_value = stored_pred["index"], stored_pred["prediction"]
#                 current_value = current_predictions[index]
                
#                 self.assertEqual(stored_value, current_value, f"Prediction at index {index} has changed! Expected {stored_value}, but got {current_value}.")

#             logging.debug("Model predictions are consistent with previously stored predictions.")


    def test_regression(self):
        """
        Regression test to ensure consistent model results.
        """
        # Load the previously stored predictions (if any)
        previous_predictions = self.load_previous_predictions()
        
        # Run the current model to get predictions
        current_predictions = self.run_current_model()

        # If there are no previous predictions, store the current predictions
        if previous_predictions is None:
            self.store_predictions(current_predictions)
        else:
            # Compare the current predictions with the previous predictions
            self.assertPredictionsMatch(previous_predictions, current_predictions)

    def load_previous_predictions(self):
        # Retrieve stored predictions from the database
        # Using get_analysis_timeseries_db as an example, replace with the correct method if needed
        db = edb.get_analysis_timeseries_db()
        predictions = db.find_one({"user_id": self.user_id, "metadata.key": "predictions"})
        return predictions

    def run_current_model(self):
        # Placeholder: Run the current model and get predictions
        # Replace this with the actual model running code
        predictions = None
        return predictions

    def store_predictions(self, predictions):
        # Store the predictions in the database
        # Using get_analysis_timeseries_db as an example, replace with the correct method if needed
        db = edb.get_analysis_timeseries_db()
        entry = {
            "user_id": self.user_id,
            "metadata": {
                "key": "predictions",
                "write_ts": pd.Timestamp.now().timestamp()  # Using pandas timestamp as an example
            },
            "data": predictions
        }
        db.insert_one(entry)

    def assertPredictionsMatch(self, prev, curr):
        # Placeholder: Check if the predictions match
        # This will depend on the format and type of your predictions
        # For example, if predictions are lists or arrays, you can use numpy
        if not np.array_equal(prev, curr):
            self.fail("Current model predictions do not match previously stored predictions!")
