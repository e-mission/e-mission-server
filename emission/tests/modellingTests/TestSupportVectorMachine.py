import unittest
import emission.analysis.modelling.trip_model.support_vector_machine as eamts
import emission.tests.modellingTests.modellingTestAssets as etmm
import logging
import pandas as pd
import random


class TestSupportVectorMachine(unittest.TestCase):

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
            "dependent_var": {
                "name": "data.user_input.replaced_mode",
                "classes": [
                    "drive",
                    "walk",
                    "bike",
                    "transit"
                ]
            }
        }
        model = eamts.SupportVectorMachine(model_config)
        model.fit(trips)
        model.predict(trips)


    def testUnseenFeatures(self):
        """
        if the input classes for a feature change throw sklearn error
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
            "dependent_var": {
                "name": "data.user_input.replaced_mode",
                "classes": [
                    "drive",
                    "walk",
                    "bike",
                    "transit"
                ]
            }
        }
        model = eamts.SupportVectorMachine(model_config)
        model.fit(train_trips)

        with self.assertRaises(ValueError):
            model.predict(test_trips)


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
                    "walk",
                    "bike",
                    "transit"
                ],
                "data.user_input.purpose_confirm": [
                    "work",
                    "home",
                    "school"
                ],
                "data.distance": None
            },
            "dependent_var": {
                "name": "data.user_input.replaced_mode",
                "classes": [
                    "drive",
                    "walk",
                    "bike",
                    "transit"
                ]
            }
        }
        model = eamts.SupportVectorMachine(model_config)
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
        survey_data = {
           "group_hg4zz25.How_old_are_you": ['0___25_years_old', '26___55_years_old', '56___70_years_old'],
           "group_hg4zz25.Are_you_a_student": ['not_a_student', 'yes'],
           "group_pa5ah98.Please_identify_which_category": ['0_to__49_999', '_50_000_to__99_999', '100_000_or_more']
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
            survey_data=survey_data, 
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
                ],
                "data.distance": None,
                "data.jsonDocResponse.group_hg4zz25.How_old_are_you": [
                    '0___25_years_old',
                    '26___55_years_old',
                    '56___70_years_old'
                ],
                "data.jsonDocResponse.group_hg4zz25.Are_you_a_student": [
                    'not_a_student',
                    'yes'
                ],
                "data.jsonDocResponse.group_pa5ah98.Please_identify_which_category": [
                    '0_to__49_999',
                    '_50_000_to__99_999',
                    '100_000_or_more'
                ]
            },
            "dependent_var": {
                "name": "data.user_input.replaced_mode",
                "classes": [
                    "drive",
                    "walk",
                    "bike",
                    "transit"
                ]
            }
        }
        model = eamts.SupportVectorMachine(model_config)
        model.fit(trips)
        y = model.predict(trips)

        # No class in predictions that's not in training data
        for predicted_class in pd.unique(y):
            self.assertIn(predicted_class, model_config['dependent_var']['classes'])


    def testIncremental(self):
        """
        the model should fit and predict incrementally on normal data without errors
        """
        label_data = {
            "mode_confirm": ['walk', 'bike', 'transit'],
            "purpose_confirm": ['work', 'home', 'school'],
            "replaced_mode": ['drive','walk']
        }
        # generate $n trips.
        n = 20
        m = 5
        initial_trips = etmm.generate_mock_trips(
            user_id="joe", 
            trips=n, 
            origin=(0, 0), 
            destination=(1, 1), 
            label_data=label_data, 
            within_threshold=m, 
            threshold=0.001,  # ~ 111 meters in degrees WGS84
        )
        additional_trips = etmm.generate_mock_trips(
            user_id="joe", 
            trips=n*5, 
            origin=(0, 0), 
            destination=(1, 1), 
            label_data=label_data, 
            within_threshold=m, 
            threshold=0.001,  # ~ 111 meters in degrees WGS84
        )
        # pass in a test configuration
        model_config = {
            "incremental_evaluation": True,
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
            "dependent_var": {
                "name": "data.user_input.replaced_mode",
                "classes": [
                    "drive",
                    "walk",
                    "bike",
                    "transit"
                ]
            }
        }
        model = eamts.SupportVectorMachine(model_config)
        # Start with some initialization data
        model.fit(initial_trips)
        # Train on additional sets of data and predict for initial data
        for i in range(0, 5):
            model.fit(additional_trips[i:(i+1)*n])
            model.predict(initial_trips)


    def testUnseenClassesIncremental(self):
        """
        if the input classes for a feature change throw sklearn error
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
        initial_trips = etmm.generate_mock_trips(
            user_id="joe", 
            trips=n, 
            origin=(0, 0), 
            destination=(1, 1), 
            label_data=train_label_data, 
            within_threshold=m, 
            threshold=0.001,  # ~ 111 meters in degrees WGS84
        )
        additional_trips = etmm.generate_mock_trips(
            user_id="joe", 
            trips=n*5, 
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
            "dependent_var": {
                "name": "data.user_input.replaced_mode",
                "classes": [
                    "drive",
                    "walk",
                    "bike",
                    "transit"
                ]
            }
        }
        model = eamts.SupportVectorMachine(model_config)
        # Start with some initialization data
        model.fit(initial_trips)
        # Train on additional sets of data
        for i in range(0, 5):
            model.fit(additional_trips[i:(i+1)*n])

        # If an unseen class is introduced, allow sklearn to throw error
        with self.assertRaises(ValueError):
            model.predict(test_trips)        


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
            "dependent_var": {
                "name": "data.user_input.replaced_mode",
                "classes": [
                    "drive",
                    "walk",
                    "bike",
                    "transit"
                ]
            }
        }
        model = eamts.SupportVectorMachine(model_config)
        # there is a separate random number generator in SGDClassifier that 
        # must be fixed to get consistent predictions
        model.svm.random_state = (3)
        model.fit(trips)
        y = model.predict(trips)

        # Test that predicted == expected
        # note that it seems with a small dataset the svm tends to predict a single category
        expected_result = [
            'transit', 'transit', 'bike', 'transit', 'transit', 'bike', 'transit', 'transit',
            'transit', 'transit', 'bike', 'transit', 'transit', 'transit', 'transit',
            'transit', 'transit', 'transit', 'bike', 'bike'
        ]
        print(y)
        for i, prediction in enumerate(y):
            self.assertEqual(prediction, expected_result[i])
