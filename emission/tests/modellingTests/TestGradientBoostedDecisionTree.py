import unittest
import emission.analysis.modelling.trip_model.gradient_boosted_decision_tree as eamtg
import emission.tests.modellingTests.modellingTestAssets as etmm
import logging


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
            "feature_list": ['mode_confirm','purpose_confirm'],
            "dependent_var": 'replaced_mode'
        }
        model = eamtg.GradientBoostedDecisionTree(model_config)
        model.fit(trips)
        model.predict(trips)

    def testUnseenTrainingClasses(self):
        """
        if a new class is added the model should re-train
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
            "feature_list": ['mode_confirm','purpose_confirm'],
            "dependent_var": 'replaced_mode'
        }
        model = eamtg.GradientBoostedDecisionTree(model_config)
        model.fit(trips)
        model.predict(trips)

        # any predicted values must 
        # self.assertTrue(at_least_one_large_bin, "at least one bin should have at least 5 features in it")
