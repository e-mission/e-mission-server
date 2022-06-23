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
        label_data = {
            "mode_labels": ['walk', 'bike', 'transit'],
            "purpose_labels": ['work', 'home', 'school'],
            "replaced_mode_labels": ['drive']
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
            has_label_p=1.0
        )
        model = eamtg.GreedySimilarityBinning(
            metric=eamso.OriginDestinationSimilarity(),
            sim_thresh=500,      # meters,
            apply_cutoff=False  # currently unused 
        )
        
        model.fit(trips)

        # 5 trip features should appear together in one bin
        at_least_one_large_bin = any(map(lambda b: len(b['features']) >= should_be_grouped, model.bins.values()))
        self.assertTrue(at_least_one_large_bin, "at least one bin should have at least 5 features in it")

    def testPrediction(self):
        label_data = {
            "mode_labels": ['skipping'],
            "purpose_labels": ['pizza_party'],
            "replaced_mode_labels": ['crabwalking']
        }

        n = 6
        trips = etmm.generate_mock_trips(
            user_id="joe", 
            trips=n, 
            origin=(0, 0), 
            destination=(1, 1), 
            label_data=label_data, 
            threshold=0.001,  # ~ 111 meters in degrees WGS84
            has_label_p=1.0
        )
        model = eamtg.GreedySimilarityBinning(
            metric=eamso.OriginDestinationSimilarity(),
            sim_thresh=500,      # meters,
            apply_cutoff=False  # currently unused 
        )
        
        train = trips[0:5]
        test = trips[5]

        model.fit(train)
        results, n = model.predict(test)

        self.assertEqual(len(results), 1, "should have found a matching bin")
        self.assertEqual(n, len(train), "that bin should have had the whole train set in it")
