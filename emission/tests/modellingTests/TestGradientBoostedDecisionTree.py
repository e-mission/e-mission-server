import unittest
import emission.analysis.modelling.trip_model.gradient_boosted_decision_tree as eamtg
import emission.tests.modellingTests.modellingTestAssets as etmm
import logging
import pandas as pd
import random


class TestGradientBoostedDecisionTree(unittest.TestCase):

    def setUp(self) -> None:
        logging.basicConfig(format='%(asctime)s:%(levelname)s:%(message)s',
        level=logging.DEBUG)


    def testSmoke(self):
        """
        the model should fit and predict on normal data without errors
        """
        label_data = {
            "mode_confirm": ['walk', 'bike', 'transit'],
            "purpose_confirm": ['work', 'home', 'school'],
            "replaced_mode": ['drive','walk']
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
            within_threshold=m, 
            threshold=0.001,  # ~ 111 meters in degrees WGS84
        )
        # pass in a test configuration
        model_config = {
            "incremental_evaluation": False,
            "feature_list": {
                "data.user_input.mode_confirm": [
                    "walk",
                    "bike",
                    "transit"
                ],
                "data.user_input.purpose_confirm": [
                    "work",
                    "home",
                    "school"
                ]
            },
            "dependent_var": "data.user_input.replaced_mode"
        }
        model = eamtg.GradientBoostedDecisionTree(model_config)
        model.fit(trips)
        model.predict(trips)


    def testUnseenFeatures(self):
        """
        if the input classes for a feature change throw sklearn error
        the test mode_confirm includes 'drive' which is not in the training set nor config
        """
        train_label_data = {
            "mode_confirm": ['walk', 'bike', 'transit'],
            "purpose_confirm": ['work', 'home', 'school'],
            "replaced_mode": ['drive','walk']
        }
        test_label_data = {
            "mode_confirm": ['drive'],
            "purpose_confirm": ['work', 'home', 'school'],
            "replaced_mode": ['drive','walk']
        }
        # generate $n trips.
        n = 20
        m = 5
        train_trips = etmm.generate_mock_trips(
            user_id="joe", 
            trips=n, 
            origin=(0, 0), 
            destination=(1, 1), 
            label_data=train_label_data, 
            within_threshold=m, 
            threshold=0.001,  # ~ 111 meters in degrees WGS84
        )
        test_trips = etmm.generate_mock_trips(
            user_id="joe", 
            trips=n, 
            origin=(0, 0), 
            destination=(1, 1), 
            label_data=test_label_data, 
            within_threshold=m, 
            threshold=0.001,  # ~ 111 meters in degrees WGS84
        )
        # pass in a test configuration
        model_config = {
            "incremental_evaluation": False,
            "feature_list": {
                "data.user_input.mode_confirm": [
                    'walk',
                    'bike',
                    'transit'
                ],
                "data.user_input.purpose_confirm": [
                    'work',
                    'home',
                    'school'
                ]
            },
            "dependent_var": 'data.user_input.replaced_mode'
        }
        model = eamtg.GradientBoostedDecisionTree(model_config)
        model.fit(train_trips)

        with self.assertRaises(ValueError):
            y = model.predict(test_trips)


    def testNumeric(self):
        """
        the model should handle numeric and categorical variable types
        """
        label_data = {
            "mode_confirm": ['walk', 'bike', 'transit'],
            "purpose_confirm": ['work', 'home', 'school'],
            "replaced_mode": ['drive','walk']
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
            within_threshold=m, 
            threshold=0.001,  # ~ 111 meters in degrees WGS84
        )
        # pass in a test configuration
        model_config = {
            "incremental_evaluation": False,
            "feature_list": {
                "data.user_input.mode_confirm": [
                    'walk',
                    'bike',
                    'transit'
                ],
                "data.user_input.purpose_confirm": [
                    'work',
                    'home',
                    'school'
                ],
                "data.distance": None
            },
            "dependent_var": 'data.user_input.replaced_mode'
        }
        model = eamtg.GradientBoostedDecisionTree(model_config)
        X_train, y_train = model.extract_features(trips)
        # 3 features for mode confirm, 3 for trip purpose, 1 for distance
        self.assertEqual(len(X_train.columns), 7)
        # all feature columns should be strictly numeric
        self.assertTrue(X_train.apply(lambda s: pd.to_numeric(s, errors='coerce').notnull().all()).all())


    def testFull(self):
        """
        the model should handle survey, trip, and user input features
        """
        label_data = {
            "mode_confirm": ['walk', 'bike', 'transit'],
            "purpose_confirm": ['work', 'home', 'school'],
            "replaced_mode": ['drive','walk','bike','transit']
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
            within_threshold=m, 
            threshold=0.001,  # ~ 111 meters in degrees WGS84
        )
        # pass in a test configuration
        model_config = {
            "incremental_evaluation": False,
            "feature_list": {
                "data.user_input.mode_confirm": [
                    'walk',
                    'bike',
                    'transit'
                ],
                "data.user_input.purpose_confirm": [
                    'work',
                    'home',
                    'school'
                ],
                "data.distance": None,
                "data.survey.age": None,
                "data.survey.hhinc": [
                    '0-24999',
                    '25000-49000',
                    '50000-99999',
                    '100000+'
                ]
            },
            "dependent_var": 'data.user_input.replaced_mode'
        }
        model = eamtg.GradientBoostedDecisionTree(model_config)
        model.fit(trips)
        y = model.predict(trips)

        # No class in predictions that's not in training data
        for predicted_class in pd.unique(y):
            self.assertIn(predicted_class, pd.unique(pd.json_normalize(trips)[model_config['dependent_var']]))


    def testPredictions(self):
        """
        with a fixed seed, the model should make consistent predictions
        """
        random.seed(42)
        label_data = {
            "mode_confirm": ['walk', 'bike', 'transit'],
            "purpose_confirm": ['work', 'home', 'school'],
            "replaced_mode": ['drive','walk','bike','transit']
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
            within_threshold=m, 
            threshold=0.001,  # ~ 111 meters in degrees WGS84
        )
        # pass in a test configuration
        model_config = {
            "incremental_evaluation": False,
            "feature_list": {
                "data.user_input.mode_confirm": [
                    'walk',
                    'bike',
                    'transit'
                ],
                "data.user_input.purpose_confirm": [
                    'work',
                    'home',
                    'school'
                ]
            },
            "dependent_var": 'data.user_input.replaced_mode'
        }
        model = eamtg.GradientBoostedDecisionTree(model_config)
        model.fit(trips)
        y = model.predict(trips)

        # Test that predicted == expected
        expected_result = [
            'transit', 'transit', 'walk', 'transit', 'drive', 'walk', 'bike', 'transit',
            'transit', 'transit', 'walk', 'drive', 'drive', 'drive', 'drive', 'drive',
            'transit', 'transit', 'walk', 'walk'
        ]
        for i, prediction in enumerate(y):
            self.assertEqual(prediction, expected_result[i])
