import unittest
import emission.analysis.modelling.tour_model_first_only.load_predict as lp
import emission.analysis.modelling.tour_model.similarity as oursim
import emission.analysis.modelling.trip_model.greedy_similarity_binning as eamtg
import emission.tests.modellingTests.modellingTestAssets as etmm
import emission.analysis.modelling.similarity.od_similarity as eamso
import json
import logging
import numpy as np
import pandas as pd
import emission.core.common as ecc
import emission.core.wrapper.entry as ecwe

#
# Test to see if the new implementations are consistent with the old implementations
#

class TestBackwardsCompat(unittest.TestCase):
    def setUp(self) -> None:
        logging.basicConfig(format='%(asctime)s:%(levelname)s:%(message)s',
        level=logging.DEBUG)

    def testAnyVsAllWhilePredicting(self):
        trip_coords = (8,12)
        trips = []
        for i in range(trip_coords[0], trip_coords[1], 1):
            trips.append(ecwe.Entry({"data": {"start_loc": {"coordinates": [i/10,i/10]},
                "end_loc": {"coordinates": [i/10+0.1, i/10+0.1]},
                "user_input": {"mode_confirm": "walk", "purpose_confirm": "exercise"}},
                "metadata": {"key": "analysis/confirmed_trip"}}))
        distanceMatrix = np.zeros((len(trips), len(trips)))
        for i, trip1 in enumerate(trips):
            for j, trip2 in enumerate(trips):
                distanceMatrix[i][j] = ecc.calDistance(
                    trip1.data.start_loc["coordinates"],
                    trip2.data.start_loc["coordinates"])
        logging.debug("For the test trips, distance matrix is")
        logging.debug("%s" % pd.DataFrame(distanceMatrix))

# 0             1             2             3             4
# 0      0.000000  15724.471142  31448.726739  47172.742840  62896.495491
# 1  15724.471142      0.000000  15724.255604  31448.271720  47172.024395
# 2  31448.726739  15724.255604      0.000000  15724.016124  31447.768817
# 3  47172.742840  31448.271720  15724.016124      0.000000  15723.752703
# 4  62896.495491  47172.024395  31447.768817  15723.752703      0.000000
# .
# So let's pick a threshold of 16000. With the "any" approach, all of them will
# be in one bin, with the "all" approach, we will end up with multiple bins
        old_builder = oursim.similarity(trips, 16000,
            shouldFilter=False, cutoff=False)
        old_builder.bin_data()
        old_bins = old_builder.bins
        logging.debug("old bins = %s" % old_bins)
# old bins = [[0, 1], [2, 3]]

        model_config = {
            "metric": "od_similarity",
            "similarity_threshold_meters": 16000,      # meters,
            "apply_cutoff": False,
            "incremental_evaluation": False
        }
        new_builder = eamtg.GreedySimilarityBinning(model_config)
        new_builder.fit(trips)
        new_bins = new_builder.bins
        logging.debug("new bins = %s" % new_bins)
        self.assertEqual(len(old_bins), len(new_bins),
            f"old bins = {old_bins} but new_bins = {new_bins}")


if __name__ == '__main__':
    unittest.main()


