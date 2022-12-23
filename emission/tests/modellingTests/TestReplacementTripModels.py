import unittest
import emission.core.get_database as edb
import emission.analysis.modelling.trip_model.gradient_boosted_decision_tree as eamtg
import emission.analysis.modelling.trip_model.support_vector_machine as eamts
import emission.tests.modellingTests.modellingTestAssets as etmm
import logging
import pandas as pd
import random


class TestReplacementTripModels(unittest.TestCase):

    def setUp(self) -> None:
        logging.basicConfig(format='%(asctime)s:%(levelname)s:%(message)s',
        level=logging.DEBUG)


    def testSmoke(self):
        """
        the model should fit and predict on normal data without errors
        """
        # though we cannot use mode_confirm or purpose_confirm to predict, they are required for mock trip generation
        # for now, just pass it to user-label and sensed-label data
        label_data = {
            "mode_confirm": ['drive'],
            "replaced_mode": ['drive','walk'],
            "purpose_confirm": ['walk']
        }
        # generate $n trips.
        n = 20
        m = 5
        trips = etmm.generate_mock_trips(
            user_id="joe", 
            trips=n, 
            origin=(0, 0), 
            destination=(1, 1), 
            label_data=label_data, 
            sensed_label_data=label_data, 
            within_threshold=m, 
            threshold=0.001,  # ~ 111 meters in degrees WGS84
        )
        # pass in a test configuration
        model_config = {
            "incremental_evaluation": False,
            "feature_list": {
                "data.inferred_labels.mode_confirm": [
                    "walk",
                    "bike",
                    "drive"
                ]
            },
            "dependent_var": {
                "name": "data.user_input.replaced_mode",
                "classes": [
                    "walk",
                    "bike",
                    "drive",
                ]
            }
        }
        model = eamtg.GradientBoostedDecisionTree(model_config)
        model.fit(trips)
        model.predict(trips)
        model = eamts.SupportVectorMachine(model_config)
        model.fit(trips)
        model.predict(trips)


#TODO: These tests were written using a prior version of the model which did not include the sensed data.
# So sensed features were actually being read from the 'user_input' key rather than 'inferred_labels'
# The above test shows how these can be set up to read from inferred labels, however they are dependent on the 
# 'add_sensed_labels()' function in modellingTestAssets.py. Once that function correctly samples the labels
# (instead of just using drive for every label) these tests should work again. The exception is 'testFull' which
# will rely on a new function which adds mock-demographic-data to the database. See todo in get_survey_df() under util.py
# for more info on that.

    # def testUnseenFeatures(self):
    #     """
    #     if the input classes for a feature change throw sklearn error
    #     """
    #     train_label_data = {
    #         "mode_confirm": ['drive'],
    #         "purpose_confirm": ['work', 'home', 'school'],
    #         "replaced_mode": ['drive','walk']
    #     }
    #     test_label_data = {
    #         "mode_confirm": ['walk','transit'],
    #         "purpose_confirm": ['work', 'home', 'school'],
    #         "replaced_mode": ['drive','walk']
    #     }
    #     # generate $n trips.
    #     n = 20
    #     m = 5
    #     train_trips = etmm.generate_mock_trips(
    #         user_id="joe", 
    #         trips=n, 
    #         origin=(0, 0), 
    #         destination=(1, 1), 
    #         label_data=train_label_data, 
    #         sensed_label_data=train_label_data,
    #         within_threshold=m, 
    #         threshold=0.001,  # ~ 111 meters in degrees WGS84
    #     )
    #     test_trips = etmm.generate_mock_trips(
    #         user_id="joe", 
    #         trips=n, 
    #         origin=(0, 0), 
    #         destination=(1, 1), 
    #         label_data=test_label_data, 
    #         sensed_label_data=test_label_data,
    #         within_threshold=m, 
    #         threshold=0.001,  # ~ 111 meters in degrees WGS84
    #     )
    #     # pass in a test configuration
    #     model_config = {
    #         "incremental_evaluation": False,
    #         "feature_list": {
    #             "data.inferred_labels.mode_confirm": [
    #                 "drive"
    #             ]
    #         },
    #         "dependent_var": {
    #             "name": "data.user_input.replaced_mode",
    #             "classes": [
    #                 "drive",
    #                 "walk"
    #             ]
    #         }
    #     }
    #     model = eamtg.GradientBoostedDecisionTree(model_config)
    #     model.fit(train_trips)
    #     with self.assertRaises(ValueError):
    #         model.predict(test_trips)
    #     model = eamts.SupportVectorMachine(model_config)
    #     model.fit(train_trips)
    #     with self.assertRaises(ValueError):
    #         model.predict(test_trips)


    # def testFull(self):
    #     """
    #     the model should handle survey, trip, and user input features
    #     """
    #     label_data = {
    #         "mode_confirm": ['walk', 'bike', 'transit'],
    #         "purpose_confirm": ['work', 'home', 'school'],
    #         "replaced_mode": ['drive','walk','bike','transit']
    #     }
    #     # generate $n trips.
    #     n = 20
    #     m = 5
    #     # for the sake of testing, need a UUID that correlates to a survey response in the database; use the first one
    #     all_survey_results = list(edb.get_timeseries_db().find({"metadata.key": "manual/demographic_survey"}))
    #     sample_uuid = all_survey_results[0]['user_id']
    #     trips = etmm.generate_mock_trips(
    #         user_id=sample_uuid, 
    #         trips=n, 
    #         origin=(0, 0), 
    #         destination=(1, 1), 
    #         label_data=label_data, 
    #         within_threshold=m, 
    #         threshold=0.001,  # ~ 111 meters in degrees WGS84
    #     )
    #     print(trips[0])
    #     # pass in a test configuration
    #     model_config = {
    #         "incremental_evaluation": False,
    #         "feature_list": {
    #             "data.inferred_labels.mode_confirm": [
    #                 "walk",
    #                 "bike",
    #                 "transit"
    #             ],
    #             "data.distance": None,
    #             "survey.group_hg4zz25.How_old_are_you": [
    #                 '0___25_years_old',
    #                 '26___55_years_old',
    #                 '56___70_years_old'
    #             ],
    #             "survey.group_hg4zz25.Are_you_a_student": [
    #                 'not_a_student',
    #                 'yes'
    #             ],
    #             "survey.group_pa5ah98.Please_identify_which_category": [
    #                 '0_to__49_999',
    #                 '_50_000_to__99_999',
    #                 '100_000_or_more'
    #             ]
    #         },
    #         "dependent_var": {
    #             "name": "data.user_input.replaced_mode",
    #             "classes": [
    #                 "drive",
    #                 "walk",
    #                 "bike",
    #                 "transit"
    #             ]
    #         }
    #     }
    #     model = eamtg.GradientBoostedDecisionTree(model_config)
    #     model.fit(trips)
    #     y = model.predict(trips)
    #     # No class in predictions that's not in training data
    #     for predicted_class in pd.unique(y):
    #         self.assertIn(predicted_class, model_config['dependent_var']['classes'])

    #     model = eamts.SupportVectorMachine(model_config)
    #     model.fit(trips)
    #     y = model.predict(trips)
    #     # No class in predictions that's not in training data
    #     for predicted_class in pd.unique(y):
    #         self.assertIn(predicted_class, model_config['dependent_var']['classes'])


    # def testIncremental(self):
    #     """
    #     the model should fit and predict incrementally on normal data without errors
    #     """
    #     label_data = {
    #         "mode_confirm": ['walk', 'bike', 'transit'],
    #         "purpose_confirm": ['work', 'home', 'school'],
    #         "replaced_mode": ['drive','walk']
    #     }
    #     # generate $n trips.
    #     n = 20
    #     m = 5
    #     initial_trips = etmm.generate_mock_trips(
    #         user_id="joe", 
    #         trips=n, 
    #         origin=(0, 0), 
    #         destination=(1, 1), 
    #         label_data=label_data, 
    #         within_threshold=m, 
    #         threshold=0.001,  # ~ 111 meters in degrees WGS84
    #     )
    #     additional_trips = etmm.generate_mock_trips(
    #         user_id="joe", 
    #         trips=n*5, 
    #         origin=(0, 0), 
    #         destination=(1, 1), 
    #         label_data=label_data, 
    #         within_threshold=m, 
    #         threshold=0.001,  # ~ 111 meters in degrees WGS84
    #     )
    #     # pass in a test configuration
    #     model_config = {
    #         "incremental_evaluation": True,
    #         "feature_list": {
    #             "data.inferred_labels.mode_confirm": [
    #                 "walk",
    #                 "bike",
    #                 "transit"
    #             ]
    #         },
    #         "dependent_var": {
    #             "name": "data.user_input.replaced_mode",
    #             "classes": [
    #                 "drive",
    #                 "walk",
    #                 "bike",
    #                 "transit"
    #             ]
    #         }
    #     }
    #     model = eamts.SupportVectorMachine(model_config)
    #     # Start with some initialization data
    #     model.fit(initial_trips)
    #     # Train on additional sets of data and predict for initial data
    #     for i in range(0, 5):
    #         model.fit(additional_trips[i:(i+1)*n])
    #         model.predict(initial_trips)


    # def testUnseenClassesIncremental(self):
    #     """
    #     if the input classes for a feature change throw sklearn error
    #     """
    #     train_label_data = {
    #         "mode_confirm": ['walk', 'bike', 'transit'],
    #         "purpose_confirm": ['work', 'home', 'school'],
    #         "replaced_mode": ['drive','walk']
    #     }
    #     test_label_data = {
    #         "mode_confirm": ['drive'],
    #         "purpose_confirm": ['work', 'home', 'school'],
    #         "replaced_mode": ['drive','walk']
    #     }
    #     # generate $n trips.
    #     n = 20
    #     m = 5
    #     initial_trips = etmm.generate_mock_trips(
    #         user_id="joe", 
    #         trips=n, 
    #         origin=(0, 0), 
    #         destination=(1, 1), 
    #         label_data=train_label_data, 
    #         within_threshold=m, 
    #         threshold=0.001,  # ~ 111 meters in degrees WGS84
    #     )
    #     additional_trips = etmm.generate_mock_trips(
    #         user_id="joe", 
    #         trips=n*5, 
    #         origin=(0, 0), 
    #         destination=(1, 1), 
    #         label_data=train_label_data, 
    #         within_threshold=m, 
    #         threshold=0.001,  # ~ 111 meters in degrees WGS84
    #     )
    #     test_trips = etmm.generate_mock_trips(
    #         user_id="joe", 
    #         trips=n, 
    #         origin=(0, 0), 
    #         destination=(1, 1), 
    #         label_data=test_label_data, 
    #         within_threshold=m, 
    #         threshold=0.001,  # ~ 111 meters in degrees WGS84
    #     )
    #     # pass in a test configuration
    #     model_config = {
    #         "incremental_evaluation": False,
    #         "feature_list": {
    #             "data.inferred_labels.mode_confirm": [
    #                 "walk",
    #                 "bike",
    #                 "transit"
    #             ]
    #         },
    #         "dependent_var": {
    #             "name": "data.user_input.replaced_mode",
    #             "classes": [
    #                 "drive",
    #                 "walk",
    #                 "bike",
    #                 "transit"
    #             ]
    #         }
    #     }
    #     model = eamts.SupportVectorMachine(model_config)
    #     # Start with some initialization data
    #     model.fit(initial_trips)
    #     # Train on additional sets of data
    #     for i in range(0, 5):
    #         model.fit(additional_trips[i:(i+1)*n])
    #     # If an unseen class is introduced, allow sklearn to throw error
    #     with self.assertRaises(ValueError):
    #         model.predict(test_trips)        


    # def testPredictions(self):
    #     """
    #     with a fixed seed, the model should make consistent predictions
    #     """
    #     random.seed(42)
    #     label_data = {
    #         "mode_confirm": ['walk', 'bike', 'transit'],
    #         "purpose_confirm": ['work', 'home', 'school'],
    #         "replaced_mode": ['drive','walk','bike','transit']
    #     }
    #     # generate $n trips.
    #     n = 20
    #     m = 5
    #     trips = etmm.generate_mock_trips(
    #         user_id="joe", 
    #         trips=n, 
    #         origin=(0, 0), 
    #         destination=(1, 1), 
    #         label_data=label_data, 
    #         within_threshold=m, 
    #         threshold=0.001,  # ~ 111 meters in degrees WGS84
    #     )
    #     # pass in a test configuration
    #     model_config = {
    #         "incremental_evaluation": False,
    #         "feature_list": {
    #             "data.inferred_labels.mode_confirm": [
    #                 "walk",
    #                 "bike",
    #                 "transit"
    #             ]
    #         },
    #         "dependent_var": {
    #             "name": "data.user_input.replaced_mode",
    #             "classes": [
    #                 "drive",
    #                 "walk",
    #                 "bike",
    #                 "transit"
    #             ]
    #         }
    #     }
    #     model = eamtg.GradientBoostedDecisionTree(model_config)
    #     model.fit(trips)
    #     y = model.predict(trips)
    #     # Test that predicted == expected
    #     expected_result = [
    #         'transit', 'bike', 'transit', 'bike', 'transit', 'bike', 'drive', 'transit',
    #         'transit', 'drive', 'transit', 'transit', 'bike', 'bike', 'bike', 'transit',
    #         'transit', 'transit', 'bike', 'drive'
    #     ]
    #     for i, prediction in enumerate(y):
    #         self.assertEqual(prediction, expected_result[i])

    #     model = eamts.SupportVectorMachine(model_config)
    #     # there is a separate random number generator in SGDClassifier that 
    #     # must be fixed to get consistent predictions
    #     model.svm.random_state = (3)
    #     model.fit(trips)
    #     y = model.predict(trips)
    #     # Test that predicted == expected
    #     # note that it seems with a small dataset the svm tends to predict a single category
    #     expected_result = [
    #         'drive', 'drive', 'transit', 'drive', 'drive', 'drive', 'drive', 'transit',
    #         'transit', 'drive', 'drive', 'transit', 'drive', 'drive', 'drive', 'transit',
    #         'drive', 'transit', 'drive', 'drive'
    #     ]
    #     for i, prediction in enumerate(y):
    #         self.assertEqual(prediction, expected_result[i])
