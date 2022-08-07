import unittest
import emission.analysis.modelling.tour_model_first_only.load_predict as lp
import emission.analysis.modelling.tour_model.similarity as oursim
import emission.analysis.modelling.trip_model.greedy_similarity_binning as eamtg
import emission.tests.modellingTests.modellingTestAssets as etmm
import emission.analysis.modelling.similarity.od_similarity as eamso
import emission.analysis.modelling.tour_model_first_only.build_save_model as eamtb
import emission.analysis.modelling.tour_model_first_only.load_predict as eamtl
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

    @staticmethod
    def old_predict_with_n(trip, bin_locations, user_labels, cluster_sizes, RADIUS):
        logging.debug(f"At stage: first round prediction")
        pred_bin = eamtl.find_bin(trip, bin_locations, RADIUS)
        logging.debug(f"At stage: matched with bin {pred_bin}")

        if pred_bin == -1:
            logging.info(f"No match found for {trip['data']['start_loc']} early return")
            return [], 0

        user_input_pred_list = user_labels[pred_bin]
        this_cluster_size = cluster_sizes[pred_bin]
        logging.debug(f"At stage: looked up user input {user_input_pred_list}")
        return user_input_pred_list, this_cluster_size

    def testRandomTripsWithinTheSameThreshold(self):
        label_data = {
            "mode_confirm": ['walk', 'bike', 'transit'],
            "purpose_confirm": ['work', 'home', 'school'],
            "replaced_mode": ['drive']
        }

        n = 60
        trips = etmm.generate_mock_trips(
            user_id="joe", 
            trips=n, 
            origin=(0, 0), 
            destination=(1, 1), 
            label_data=label_data, 
            threshold=0.001,  # ~ 111 meters in degrees WGS84
        )

        # These fields should ignored for the first round, but are extracted anyway
        # So let's fill them in with dummy values
        for t in trips:
            t["data"]["distance"] = 1000
            t["data"]["duration"] = 10

        train = trips[0:50]
        test = trips[50:60]

        model_config = {
            "metric": "od_similarity",
            "similarity_threshold_meters": 500,      # meters,
            "apply_cutoff": False,
            "incremental_evaluation": False
        }
        new_model = eamtg.GreedySimilarityBinning(model_config)
        new_model.fit(train)

        old_builder = oursim.similarity(train, 500,
            shouldFilter=False, cutoff=False)
        old_builder.fit()

        self.assertEqual(len(old_builder.bins), len(new_model.bins),
            f"old bins = {old_builder.bins} but new_bins = {new_model.bins}")

        self.assertEqual(len(old_builder.bins), 1,
            f"all trips within threshold, so expected one bin, found {len(old_builder.bins)}")

        old_user_inputs = eamtb.create_user_input_map(train, old_builder.bins)
        old_location_map = eamtb.create_location_map(train, old_builder.bins)
        old_cluster_sizes = {k: len(old_location_map[k]) for k in old_location_map}

        for test_trip in test:
            new_results, new_n = new_model.predict(test_trip)
            old_results, old_n = TestBackwardsCompat.old_predict_with_n(test_trip,
                old_location_map, old_user_inputs, old_cluster_sizes, 500)

            self.assertEqual(old_n, new_n,
                f"for test trip {test_trip} old n = {old_n} and new_n = {new_n}")

            self.assertEqual(old_results, new_results,
                f"for test trip {test_trip} old result = {old_results} and new result = {new_results}")

    def testRandomTripsOutsideTheSameThreshold(self):
        label_data = {
            "mode_confirm": ['walk', 'bike', 'transit'],
            "purpose_confirm": ['work', 'home', 'school'],
            "replaced_mode": ['drive']
        }

        n = 60
        trips = etmm.generate_mock_trips(
            user_id="joe", 
            trips=n, 
            origin=(0, 0), 
            destination=(1, 1), 
            label_data=label_data, 
            threshold=0.1,  # Much bigger than the 500m threshold, so we will get multiple bins
        )

        # These fields should ignored for the first round, but are extracted anyway
        # So let's fill them in with dummy values
        for t in trips:
            t["data"]["distance"] = 1000
            t["data"]["duration"] = 10

        train = trips[0:50]
        test = trips[50:60]

        model_config = {
            "metric": "od_similarity",
            "similarity_threshold_meters": 500,      # meters,
            "apply_cutoff": False,
            "incremental_evaluation": False
        }
        new_model = eamtg.GreedySimilarityBinning(model_config)
        new_model.fit(train)

        old_builder = oursim.similarity(train, 500,
            shouldFilter=False, cutoff=False)
        old_builder.fit()

        logging.debug(f"old bins = {len(old_builder.bins)} but new_bins = {len(new_model.bins)}")

        self.assertEqual(len(old_builder.bins), len(new_model.bins),
            f"old bins = {old_builder.bins} but new_bins = {new_model.bins}")

        old_user_inputs = eamtb.create_user_input_map(train, old_builder.bins)
        old_location_map = eamtb.create_location_map(train, old_builder.bins)
        old_cluster_sizes = {k: len(old_location_map[k]) for k in old_location_map}

        for test_trip in test:
            new_results, new_n = new_model.predict(test_trip)
            old_results, old_n = TestBackwardsCompat.old_predict_with_n(test_trip,
                old_location_map, old_user_inputs, old_cluster_sizes, 500)

            self.assertEqual(old_n, new_n,
                f"for test trip {test_trip} old n = {old_n} and new_n = {new_n}")

            self.assertEqual(old_results, new_results,
                f"for test trip {test_trip} old result = {old_results} and new result = {new_results}")
if __name__ == '__main__':
    unittest.main()


