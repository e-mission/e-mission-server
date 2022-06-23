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
        trips = etmm.generate_mock_trips("joe", 14, [0, 0], [1, 1], label_data, 6, has_label_p=1.0)
        model = eamtg.GreedySimilarityBinning(
            metric=eamso.OriginDestinationSimilarity(),
            sim_thresh=500,      # meters,
            apply_cutoff=False  # currently unused 
        )
        
        model.fit(trips)
        print(json.dumps(model.bins, sort_keys=True, indent=4))