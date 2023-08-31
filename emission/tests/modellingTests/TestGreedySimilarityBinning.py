import unittest
import emission.analysis.modelling.trip_model.greedy_similarity_binning as eamtg
import emission.tests.modellingTests.modellingTestAssets as etmm
import logging


class TestGreedySimilarityBinning(unittest.TestCase):

    def setUp(self) -> None:
        logging.basicConfig(format='%(asctime)s:%(levelname)s:%(message)s',
        level=logging.DEBUG)

    def testNoBinning(self):
        """
        Tests the three (origin, destination and origin-destination based) 
        binning configuration for trips.

        When both the origin and destination points of trips are outside a threshold
        limit, none of the trips should be binned with the other in any of the three 
        configs (origin, destination or origin-and-destination based).       
        """

        label_data = {
            "mode_confirm": ['walk', 'bike', 'transit'],
            "purpose_confirm": ['work', 'home', 'school'],
            "replaced_mode": ['drive']
        }

        # generate $n trips. $m of them should have origin and destinations sampled
        # within a radius that should have them binned.
        n = 20
        m = 5
        
        # trip_part: when mock trips are generated, coordinates of this part of 
        #            m trips will be within the threshold. trip_part can take one
        #            among the four values:
        #
        #            1. '__' ->(None, meaning NEITHER origin nor destination of any trip will lie 
        #             within the mentioned threshold when trips are generated),
        #
        #            2. 'o_' ->(origin, meaning ONLY origin of m trips will lie within the mentioned 
        #             threshold when trips are generated),
        #
        #            3. '_d' ->(destination),meaning ONLY destination of m trips will lie within the 
        #             mentioned threshold when trips are generated)
        #
        #            4. 'od' ->(origin and destination,meaning BOTH origin and destination of m trips
        #             will lie within the mentioned threshold when trips are generated)

        trips = etmm.generate_mock_trips(
            user_id="joe", 
            trips=n, 
            origin=(0, 0), 
            destination=(1, 1),
            trip_part='__',
            label_data=label_data, 
            within_threshold=m, 
            threshold=0.001,  # ~ 111 meters in degrees WGS84
        )
    

        # pass in a test configuration to the binning algorithm.
        #
        # clustering_way : Part of the trip used for checking pairwise proximity.
        #                  Can take one of the three values:
        #                  
        #                   1. 'origin' -> using origin of the trip to check if 2 points
        #                                   lie within the mentioned similarity_threshold_meters
        #                   2. 'destination' -> using destination of the trip to check if 2 points
        #                                       lie within the mentioned similarity_threshold_meters
        #                   3. 'origin-destination' -> both origin and destination of the trip to check 
        #                                             if 2 points lie within the mentioned 
        #                                              similarity_threshold_meters
        
        model1_config = {
            "metric": "od_similarity",
            "similarity_threshold_meters": 111,  # meters,
            "apply_cutoff": False,
            "clustering_way": "origin",  
            "incremental_evaluation": False
        }
        model1 = eamtg.GreedySimilarityBinning(model1_config)
        model1.fit(trips)


        model2_config = {
            "metric": "od_similarity",
            "similarity_threshold_meters":111,  # meters,
            "apply_cutoff": False,
            "clustering_way": "destination",
            "incremental_evaluation": False
        }
        model2 = eamtg.GreedySimilarityBinning(model2_config)
        model2.fit(trips)


        model3_config = {
            "metric": "od_similarity",
            "similarity_threshold_meters": 111,  # meters,
            "apply_cutoff": False,
            "clustering_way": "origin-destination",
            "incremental_evaluation": False
        }
        model3 = eamtg.GreedySimilarityBinning(model3_config)
        model3.fit(trips)

        # Since neither the origin nor the destination of the points generated lie
        # within the threshold, there should be no binning at all. All the bins should
        # have size 1.

        at_least_one_large_bin = all(map(lambda b: len(b['feature_rows']) == 1, model1.bins.values()))
        self.assertTrue(at_least_one_large_bin, "no bin should have more than 1 features in it")

        at_least_one_large_bin = all(map(lambda b: len(b['feature_rows']) == 1, model2.bins.values()))
        self.assertTrue(at_least_one_large_bin, "no bin should have more than 1 features in it")

        at_least_one_large_bin = all(map(lambda b: len(b['feature_rows']) == 1, model3.bins.values()))
        self.assertTrue(at_least_one_large_bin, "no bin should have more than 1 features in it")



    def testBinningByOrigin(self):
        """
        Tests the 'origin' based binning method for trips.

        When only the origin points of trips are within a threshold
        limit, trips must be binned together that too if binned based on 
        'origins', otherwise no binning.       
        """

        label_data = {
            "mode_confirm": ['walk', 'bike', 'transit'],
            "purpose_confirm": ['work', 'home', 'school'],
            "replaced_mode": ['drive']
        }

        # generate $n trips. $m of them should have origin and destinations sampled
        # within a radius that should have them binned.
        n = 20
        m = 5

        # trip_part: when mock trips are generated, coordinates of this part of 
        #            m trips will be within the threshold. trip_part can take one
        #            among the four values:
        #
        #            1. '__' ->(None, meaning NEITHER origin nor destination of any trip will lie 
        #             within the mentioned threshold when trips are generated),
        #
        #            2. 'o_' ->(origin, meaning ONLY origin of m trips will lie within the mentioned 
        #             threshold when trips are generated),
        #
        #            3. '_d' ->(destination),meaning ONLY destination of m trips will lie within the 
        #             mentioned threshold when trips are generated)
        #
        #            4. 'od' ->(origin and destination,meaning BOTH origin and destination of m trips
        #             will lie within the mentioned threshold when trips are generated)

        trips = etmm.generate_mock_trips(
            user_id="joe", 
            trips=n, 
            origin=(0, 0), 
            destination=(1, 1),
            trip_part='o_',
            label_data=label_data, 
            within_threshold=m, 
            threshold=0.001,  # ~ 111 meters in degrees WGS84
        )

        # pass in a test configuration to the binning algorithm.
        #
        # clustering_way : Part of the trip used for checking pairwise proximity.
        #                  Can take one of the three values:
        #                  
        #                   1. 'origin' -> using origin of the trip to check if 2 points
        #                                   lie within the mentioned similarity_threshold_meters
        #                   2. 'destination' -> using destination of the trip to check if 2 points
        #                                       lie within the mentioned similarity_threshold_meters
        #                   3. 'origin-destination' -> both origin and destination of the trip to check 
        #                                             if 2 points lie within the mentioned 
        #                                              similarity_threshold_meters
        
        model1_config = {
            "metric": "od_similarity",
            "similarity_threshold_meters": 111,  # meters,
            "apply_cutoff": False,
            "clustering_way": "origin",
            "incremental_evaluation": False
        }
        model1 = eamtg.GreedySimilarityBinning(model1_config)
        model1.fit(trips)


        model2_config = {
            "metric": "od_similarity",
            "similarity_threshold_meters":111,  # meters,
            "apply_cutoff": False,
            "clustering_way": "destination",
            "incremental_evaluation": False
        }
        model2 = eamtg.GreedySimilarityBinning(model2_config)
        model2.fit(trips)


        model3_config = {
            "metric": "od_similarity",
            "similarity_threshold_meters": 111,  # meters,
            "apply_cutoff": False,
            "clustering_way": "origin-destination",
            "incremental_evaluation": False
        }
        model3 = eamtg.GreedySimilarityBinning(model3_config)
        model3.fit(trips)
        

        # Since only the origin of the points generated lies within the threshold,
        # there should be binning only when 'origin' config is used. Otherwise all 
        # the bins should have size 1.

        at_least_one_large_bin = any(map(lambda b: len(b['feature_rows']) == m, model1.bins.values()))
        self.assertTrue(at_least_one_large_bin, "at least one bin should have at least 5 features in it")

        at_least_one_large_bin = all(map(lambda b: len(b['feature_rows']) ==1, model2.bins.values()))
        self.assertTrue(at_least_one_large_bin, "no bin should have more than 1 features in it")

        at_least_one_large_bin = all(map(lambda b: len(b['feature_rows']) == 1, model3.bins.values()))
        self.assertTrue(at_least_one_large_bin, "no bin should have more than 1 features in it")



    def testBinningByDestination(self):
        """
        Tests the 'destination' based binning method for trips.

        When only the destination points of trips are within a threshold
        limit, trips must be binned together that too if binned based on 
        'destination', otherwise no binning.       
        """

        label_data = {
            "mode_confirm": ['walk', 'bike', 'transit'],
            "purpose_confirm": ['work', 'home', 'school'],
            "replaced_mode": ['drive']
        }

        # generate $n trips. $m of them should have origin and destinations sampled
        # within a radius that should have them binned.
        n = 20
        m = 5

        # trip_part: when mock trips are generated, coordinates of this part of 
        #            m trips will be within the threshold. trip_part can take one
        #            among the four values:
        #
        #            1. '__' ->(None, meaning NEITHER origin nor destination of any trip will lie 
        #             within the mentioned threshold when trips are generated),
        #
        #            2. 'o_' ->(origin, meaning ONLY origin of m trips will lie within the mentioned 
        #             threshold when trips are generated),
        #
        #            3. '_d' ->(destination),meaning ONLY destination of m trips will lie within the 
        #             mentioned threshold when trips are generated)
        #
        #            4. 'od' ->(origin and destination,meaning BOTH origin and destination of m trips
        #             will lie within the mentioned threshold when trips are generated)

        trips = etmm.generate_mock_trips(
            user_id="joe", 
            trips=n, 
            origin=(0, 0), 
            destination=(1, 1),
            trip_part='_d',
            label_data=label_data, 
            within_threshold=m, 
            threshold=0.001,  # ~ 111 meters in degrees WGS84
        )

        # pass in a test configuration to the binning algorithm.
        #
        # clustering_way : Part of the trip used for checking pairwise proximity.
        #                  Can take one of the three values:
        #                  
        #                   1. 'origin' -> using origin of the trip to check if 2 points
        #                                   lie within the mentioned similarity_threshold_meters
        #                   2. 'destination' -> using destination of the trip to check if 2 points
        #                                       lie within the mentioned similarity_threshold_meters
        #                   3. 'origin-destination' -> both origin and destination of the trip to check 
        #                                             if 2 points lie within the mentioned 
        #                                              similarity_threshold_meters
        
        model1_config = {
            "metric": "od_similarity",
            "similarity_threshold_meters": 111,  # meters,
            "apply_cutoff": False,
            "clustering_way": "origin",
            "incremental_evaluation": False
        }
        model1 = eamtg.GreedySimilarityBinning(model1_config)
        model1.fit(trips)


        model2_config = {
            "metric": "od_similarity",
            "similarity_threshold_meters":111,  # meters,
            "apply_cutoff": False,
            "clustering_way": "destination",
            "incremental_evaluation": False
        }
        model2 = eamtg.GreedySimilarityBinning(model2_config)
        model2.fit(trips)


        model3_config = {
            "metric": "od_similarity",
            "similarity_threshold_meters": 111,  # meters,
            "apply_cutoff": False,
            "clustering_way": "origin-destination",
            "incremental_evaluation": False
        }
        model3 = eamtg.GreedySimilarityBinning(model3_config)
        model3.fit(trips)

        # Since only the destination of the points generated lies within the threshold,
        # there should be binning only when 'destination' config is used. Otherwise all 
        # the bins should have size 1.

        at_least_one_large_bin = all(map(lambda b: len(b['feature_rows']) == 1, model1.bins.values()))
        self.assertTrue(at_least_one_large_bin, "at least one bin should have at least 5 features in it")

        at_least_one_large_bin = any(map(lambda b: len(b['feature_rows']) ==m, model2.bins.values()))
        self.assertTrue(at_least_one_large_bin, "no bin should have more than 1 features in it")

        at_least_one_large_bin = all(map(lambda b: len(b['feature_rows']) == 1, model3.bins.values()))
        self.assertTrue(at_least_one_large_bin, "no bin should have more than 1 features in it")


    def testBinningByOriginAndDestination(self):
        """
        Tests the 'origin-destination' based binning method for trips.

        When both the origin and destination points of trips are within
        a threshold limit, trips will be binned together in all three (origin , 
        destination, origin-and-destinaiton) configurations. 
        """        

        label_data = {
            "mode_confirm": ['walk', 'bike', 'transit'],
            "purpose_confirm": ['work', 'home', 'school'],
            "replaced_mode": ['drive']
        }

        # generate $n trips. $m of them should have origin and destinations sampled
        # within a radius that should have them binned.
        n = 20
        m = 5

        # trip_part: when mock trips are generated, coordinates of this part of 
        #            m trips will be within the threshold. trip_part can take one
        #            among the four values:
        #
        #            1. '__' ->(None, meaning NEITHER origin nor destination of any trip will lie 
        #             within the mentioned threshold when trips are generated),
        #
        #            2. 'o_' ->(origin, meaning ONLY origin of m trips will lie within the mentioned 
        #             threshold when trips are generated),
        #
        #            3. '_d' ->(destination),meaning ONLY destination of m trips will lie within the 
        #             mentioned threshold when trips are generated)
        #
        #            4. 'od' ->(origin and destination,meaning BOTH origin and destination of m trips
        #             will lie within the mentioned threshold when trips are generated)

        trips = etmm.generate_mock_trips(
            user_id="joe", 
            trips=n, 
            origin=(0, 0), 
            destination=(1, 1),
            trip_part='od',
            label_data=label_data, 
            within_threshold=m, 
            threshold=0.001,  # ~ 111 meters in degrees WGS84
        )

        # pass in a test configuration to the binning algorithm.
        #
        # clustering_way : Part of the trip used for checking pairwise proximity.
        #                  Can take one of the three values:
        #                  
        #                   1. 'origin' -> using origin of the trip to check if 2 points
        #                                   lie within the mentioned similarity_threshold_meters
        #                   2. 'destination' -> using destination of the trip to check if 2 points
        #                                       lie within the mentioned similarity_threshold_meters
        #                   3. 'origin-destination' -> both origin and destination of the trip to check 
        #                                             if 2 points lie within the mentioned 
        #                                              similarity_threshold_meters
        
        model1_config = {
            "metric": "od_similarity",
            "similarity_threshold_meters": 111,  # meters,
            "apply_cutoff": False,
            "clustering_way": "origin",
            "incremental_evaluation": False
        }
        model1 = eamtg.GreedySimilarityBinning(model1_config)
        model1.fit(trips)


        model2_config = {
            "metric": "od_similarity",
            "similarity_threshold_meters":111,  # meters,
            "apply_cutoff": False,
            "clustering_way": "destination",
            "incremental_evaluation": False
        }
        model2 = eamtg.GreedySimilarityBinning(model2_config)
        model2.fit(trips)


        model3_config = {
            "metric": "od_similarity",
            "similarity_threshold_meters": 111,  # meters,
            "apply_cutoff": False,
            "clustering_way": "origin-destination",
            "incremental_evaluation": False
        }
        model3 = eamtg.GreedySimilarityBinning(model3_config)
        model3.fit(trips)

        # Since both the origin and the destination points of the generated trips lie 
        # within the threshold, there should be binning in all three configs.

        at_least_one_large_bin = any(map(lambda b: len(b['feature_rows']) == m, model1.bins.values()))
        self.assertTrue(at_least_one_large_bin, "at least one bin should have at least 5 features in it")

        at_least_one_large_bin = any(map(lambda b: len(b['feature_rows']) ==m, model2.bins.values()))
        self.assertTrue(at_least_one_large_bin, "no bin should have more than 1 features in it")

        at_least_one_large_bin = any(map(lambda b: len(b['feature_rows']) == m, model3.bins.values()))
        self.assertTrue(at_least_one_large_bin, "no bin should have more than 1 features in it")


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
            trip_part='od', 
            label_data=label_data, 
            threshold=0.001,  # ~ 111 meters in degrees WGS84
        )

        model_config = {
            "metric": "od_similarity",
            "similarity_threshold_meters": 500,      # meters,
            "apply_cutoff": False,
            "clustering_way": "origin-destination",
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
            trip_part='od',
            label_data=label_data, 
            threshold=0.001,  # ~ 111 meters in degrees WGS84
        )
        test = etmm.generate_mock_trips(
            user_id="joe", 
            trips=1, 
            origin=(61.1042262, -150.5611644),       # Anchorage, AK
            destination=(62.2721466, -150.3233046),  # Talkeetna, AK
            trip_part='od',
            label_data=label_data, 
            threshold=0.001,  # ~ 111 meters in degrees WGS84
        )

        model_config = {
            "metric": "od_similarity",
            "similarity_threshold_meters": 500,      # meters,
            "apply_cutoff": False,
            "clustering_way": "origin-destination",
            "incremental_evaluation": False
        }
        model = eamtg.GreedySimilarityBinning(model_config)

        model.fit(train)
        results, n = model.predict(test[0])

        self.assertEqual(len(results), 0, "should not have found a matching bin")
        self.assertEqual(n, 0, "the number of features in an empty bin is zero")
