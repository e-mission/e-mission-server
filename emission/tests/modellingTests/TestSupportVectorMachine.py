import unittest
import emission.analysis.modelling.trip_model.support_vector_machine as eamts
import emission.tests.modellingTests.modellingTestAssets as etmm
import logging
import pandas as pd


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
            "feature_list": [
                'data.user_input.mode_confirm',
                'data.user_input.purpose_confirm'
            ],
            "dependent_var": 'data.user_input.replaced_mode'
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
            "feature_list": [
                'data.user_input.mode_confirm',
                'data.user_input.purpose_confirm'
            ],
            "dependent_var": 'data.user_input.replaced_mode'
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
            "feature_list": [
                'data.user_input.mode_confirm',
                'data.user_input.purpose_confirm',
                'distance_miles'
            ],
            "dependent_var": 'data.user_input.replaced_mode'
        }
        model = eamts.SupportVectorMachine(model_config)
        model.fit(trips)
        model.predict(trips)


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
            "feature_list": [
                'data.user_input.mode_confirm',
                'data.user_input.purpose_confirm',
                'data.survey.age',
                'data.survey.hhinc',
                'distance_miles'
            ],
            "dependent_var": 'data.user_input.replaced_mode'
        }
        model = eamts.SupportVectorMachine(model_config)
        model.fit(trips)
        y = model.predict(trips)

        # No class in predictions that's not in training data
        for predicted_class in pd.unique(y):
            self.assertIn(predicted_class, pd.unique(pd.json_normalize(trips)[model_config['dependent_var']]))
