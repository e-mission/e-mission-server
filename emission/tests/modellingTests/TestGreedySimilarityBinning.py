import unittest
import emission.analysis.modelling.trip_model.greedy_similarity_binning as eamtg
import emission.tests.modellingTests.utilities as etmu
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

        #this generates 20 trips one-by-one, where each trip's respective origin and destination 
        # points are more than 500m away.
        trips = [ etmu.setTripConfig(1, (i, i), (i+1, i+1), 'od', 1)[0] for i in range(n)]    

        # parameters passed for testing. A list, where each element is one way of clustering
        clustering_ways_paramters= ["origin","destination","origin-destination"]
        
        #Testing each of the three clustering_ways by passing them as parameters
        for cw in clustering_ways_paramters:
            with self.subTest(clustering_way=cw):
                #initialise the binning model and fit with previously generated trips
                model = etmu.setModelConfig("od_similarity",  500,  False, cw, False)
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

        # parameters passed for testing. A list, where each element of this list takes the form 
        # [trip part to be sampled within mentioned threshold , clustering way used to check similarity]
        parameters= [["o_",'origin'],["_d",'destination'],["od",'origin-destination']]
        for tp,cw in parameters:
            with self.subTest(trip_part=tp,clustering_way=cw):
                #generate random trips using utilities
                trips = etmu.setTripConfig(trips=n, org=(0, 0), dest=(1, 1),
                                trip_part=tp, within_thr=m)
                #initialise the binning model and fit with previously generated trips
                model = etmu.setModelConfig("od_similarity",  500,  False, cw, False)
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
        trips = etmu.setTripConfig(trips=n, org=(0, 0), dest=(1, 1),
                                   trip_part='od', label_data=label_data,                                   
        )
        model = etmu.setModelConfig("od_similarity",  500,  False, "origin-destination", False)
        
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

        train = etmu.setTripConfig(trips=n, org=(39.7645187, -104.9951944), # Denver, CO
                                   dest=(39.7435206, -105.2369292),  # Golden, CO
                                   trip_part='od', label_data=label_data                                 
        )
        test = etmu.setTripConfig(trips=n, org=(61.1042262, -150.5611644), # Denver, CO
                                   dest=(62.2721466, -150.3233046),  # Golden, CO
                                   trip_part='od', label_data=label_data,                                   
        )
        model = etmu.setModelConfig("od_similarity",  500,  False, "origin-destination", False)
        model.fit(train)
        results, n = model.predict(test[0])

        self.assertEqual(len(results), 0, "should not have found a matching bin")
        self.assertEqual(n, 0, "the number of features in an empty bin is zero")
