import unittest
import emission.analysis.modelling.trip_model.greedy_similarity_binning as eamtg
import emission.tests.modellingTests.modellingTestAssets as etmm
import emission.analysis.modelling.similarity.od_similarity as eamso
import json
import logging


class TestGreedySimilarityBinning(unittest.TestCase):

    def setUp(self) -> None:
        logging.basicConfig(format='%(asctime)s:%(levelname)s:%(message)s',
        level=logging.DEBUG)

    def testBinning(self):
        """
        when $should_be_grouped trips are the same, they should appear in a bin
        """
        label_data = {
            "mode_confirm": ['walk', 'bike', 'transit'],
            "purpose_confirm": ['work', 'home', 'school'],
            "replaced_mode": ['drive']
        }

        n = 20
        should_be_grouped = 5
        trips = etmm.generate_mock_trips(
            user_id="joe", 
            trips=n, 
            origin=(0, 0), 
            destination=(1, 1), 
            label_data=label_data, 
            within_threshold=should_be_grouped, 
            threshold=0.001,  # ~ 111 meters in degrees WGS84
        )

        model_config = {
            "metric": "od_similarity",
            "similarity_threshold_meters": 500,      # meters,
            "apply_cutoff": False,
            "incremental_evaluation": False
        }
        model = eamtg.GreedySimilarityBinning(model_config)
        
        model.fit(trips)

        # $should_be_grouped trip features should appear together in one bin
        at_least_one_large_bin = any(map(lambda b: len(b['features']) >= should_be_grouped, model.bins.values()))
        self.assertTrue(at_least_one_large_bin, "at least one bin should have at least 5 features in it")

    def testPrediction(self):
        """
        training and testing with similar trips should lead to a positive bin match
        """
        label_data = {
            "mode_confirm": ['skipping'],
            "purpose_confirm": ['pizza_party'],
            "replaced_mode": ['crabwalking']
        }

        n = 6
        trips = etmm.generate_mock_trips(
            user_id="joe", 
            trips=n, 
            origin=(0, 0), 
            destination=(1, 1), 
            label_data=label_data, 
            threshold=0.001,  # ~ 111 meters in degrees WGS84
        )

        model_config = {
            "metric": "od_similarity",
            "similarity_threshold_meters": 500,      # meters,
            "apply_cutoff": False,
            "incremental_evaluation": False
        }
        model = eamtg.GreedySimilarityBinning(model_config)
        
        train = trips[0:5]
        test = trips[5]

        model.fit(train)
        results, n = model.predict(test)

        self.assertEqual(len(results), 1, "should have found a matching bin")
        self.assertEqual(n, len(train), "that bin should have had the whole train set in it")

    def testNoPrediction(self):
        """
        when trained on trips in Colorado, shouldn't have a prediction for a trip in Alaska
        """
        label_data = {
            "mode_confirm": ['skipping'],
            "purpose_confirm": ['pizza_party'],
            "replaced_mode": ['crabwalking']
        }

        n = 5
        train = etmm.generate_mock_trips(
            user_id="joe", 
            trips=n, 
            origin=(39.7645187, -104.9951944),       # Denver, CO
            destination=(39.7435206, -105.2369292),  # Golden, CO
            label_data=label_data, 
            threshold=0.001,  # ~ 111 meters in degrees WGS84
        )
        test = etmm.generate_mock_trips(
            user_id="joe", 
            trips=1, 
            origin=(61.1042262, -150.5611644),       # Anchorage, AK
            destination=(62.2721466, -150.3233046),  # Talkeetna, AK
            label_data=label_data, 
            threshold=0.001,  # ~ 111 meters in degrees WGS84
        )

        model_config = {
            "metric": "od_similarity",
            "similarity_threshold_meters": 500,      # meters,
            "apply_cutoff": False,
            "incremental_evaluation": False
        }
        model = eamtg.GreedySimilarityBinning(model_config)

        model.fit(train)
        results, n = model.predict(test[0])

        self.assertEqual(len(results), 0, "should have found a matching bin")
        self.assertEqual(n, -1, "that bin should have had the whole train set in it")
