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

        When the origin and destination points of trips are outside a threshold
        limit, none of the trips should be binned with the other in any of the three 
        configs (origin, destination or origin-and-destination based).       
        """

        # generate $n trips.
        n = 20   
        binning_threshold=500
        #this generates 20 trips one-by-one, where each trip's respective origin and destination 
        # points are more than 500m away.
 
        
        label_data = {
            "mode_confirm": ['walk', 'bike', 'transit'],
            "purpose_confirm": ['work', 'home', 'school'],
            "replaced_mode": ['drive']
        }         


        trips =etmm.generate_mock_trips(
                user_id="joe", 
                trips=n, 
                trip_part='__',
                label_data=label_data, 
                within_threshold=1, 
                threshold=binning_threshold,
                origin=(0,0),
                destination=(1,1)
            )

        # parameters passed for testing. A list, where each element is one way of clustering
        clustering_ways_paramters= ["origin","destination","origin-destination"]
        
        #Testing each of the three clustering_ways by passing them as parameters
        for cw in clustering_ways_paramters:
            with self.subTest(clustering_way=cw):
                #initialise the binning model and fit with previously generated trips
                model_config = {
                                    "metric": "od_similarity",
                                    "similarity_threshold_meters": binning_threshold,  # meters,
                                    "apply_cutoff": False,
                                    "clustering_way": cw,  
                                    "incremental_evaluation": False
                                }
                model= eamtg.GreedySimilarityBinning(model_config)
                model.fit(trips)
                #check each bins for no of trips
                no_large_bin = all(map(lambda b: len(b['feature_rows']) == 1, model.bins.values()))
                #Since all trips were sampled outside the threshold, there should be no bin
                # with more then 1 trip
                self.assertTrue(no_large_bin,"no bin should have more than 1 features in it")

    def testBinning(self):
        """
        Tests the three (origin, destination and origin-destination based) 
        binning configuration for trips.

        When the points lie within threshold ,the trips are binned together.
        """
        # generate $n trips. $m of them should have origin sampled
        # within a radius that should have them binned.
        n = 20
        m = 5
        binning_threshold=500
        label_data = {
            "mode_confirm": ['walk', 'bike', 'transit'],
            "purpose_confirm": ['work', 'home', 'school'],
            "replaced_mode": ['drive']
        }

        # parameters passed for testing. A list, where each element of this list takes the form 
        # [trip part to be sampled within mentioned threshold , clustering way used to check similarity]
        parameters= [["o_",'origin'],["_d",'destination'],["od",'origin-destination']]
        for tp,cw in parameters:
            with self.subTest(trip_part=tp,clustering_way=cw):
                #generate random trips using utilities
                trips =etmm.generate_mock_trips(
                    user_id="joe", 
                    trips=n, 
                    trip_part=tp,
                    label_data=label_data, 
                    within_threshold=m, 
                    threshold=binning_threshold,
                    origin=(0,0),
                    destination=(1,1)
                )
                #initialise the binning model and fit with previously generated trips
                model_config = {
                            "metric": "od_similarity" ,
                            "similarity_threshold_meters": binning_threshold,  # meters,
                            "apply_cutoff": False,
                            "clustering_way": cw,  
                            "incremental_evaluation": False
                 }
                model = eamtg.GreedySimilarityBinning(model_config)
                model.fit(trips)
                #check each bins for no of trips
                at_least_one_large_bin = any(map(lambda b: len(b['feature_rows']) == m, model.bins.values()))
                #Since 5 trips were sampled within the threshold, there should be one bin with 5 trips
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
        trips =etmm.generate_mock_trips(
                user_id="joe", 
                trips=n, 
                trip_part='od',
                label_data=label_data, 
                within_threshold=n, 
                threshold=500,
                origin=(0,0),
                destination=(1,1)
            )
        model_config = {
                    "metric": "od_similarity",
                    "similarity_threshold_meters": 500,  # meters,
                    "apply_cutoff": False,
                    "clustering_way": 'origin_destination',  
                    "incremental_evaluation": False
                                }
        model= eamtg.GreedySimilarityBinning(model_config)
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
        binning_threshold = 500
        train = etmm.generate_mock_trips( user_id="joe",trips=n, origin=(39.7645187, -104.9951944), # Denver, CO
                                   destination=(39.7435206, -105.2369292),  # Golden, CO
                                   trip_part='od', label_data=label_data,
                                   threshold=binning_threshold, within_threshold=n
        )
        test = etmm.generate_mock_trips( user_id="amanda",trips=n, origin=(61.1042262, -150.5611644), # Denver, CO
                                   destination=(62.2721466, -150.3233046),  # Golden, CO
                                   trip_part='od', label_data=label_data,                                   
                                    threshold=binning_threshold, within_threshold=n
        )
        model_config = {
                    "metric": "od_similarity",
                    "similarity_threshold_meters": 500,  # meters,
                    "apply_cutoff": False,
                    "clustering_way": 'origin_destination',  
                    "incremental_evaluation": False
                                }
        model= eamtg.GreedySimilarityBinning(model_config)
        model.fit(train)
        results, n = model.predict(test[0])

        self.assertEqual(len(results), 0, "should not have found a matching bin")
        self.assertEqual(n, 0, "the number of features in an empty bin is zero")
