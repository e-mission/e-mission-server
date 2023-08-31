import unittest
import emission.tests.modellingTests.modellingTestAssets as etmm
import emission.analysis.modelling.similarity.od_similarity as eamso

class TestSimilarityMetric(unittest.TestCase):

    def testODsAreSimilar(self):
        generate_points_thresh = 0.001  # approx. 111 meters
        similarity_threshold = 111  # 

        metric = eamso.OriginDestinationSimilarity()
        ## Sub-Test 1 - 3 :
        # random, but, origin and destination points are sampled within a circle and should always be < sim threshold
        # Since both origin and destination poitns lie within threshold limits,they should be similar
        # when we check by just origin or just destination or both origin-and-destination

        trips = etmm.generate_mock_trips('bob', 2, [0, 0], [1, 1], 'od',threshold=generate_points_thresh) 
        coords0 = metric.extract_features(trips[0])
        coords1 = metric.extract_features(trips[1])        
        similarOD1 = metric.similar(coords0, coords1, similarity_threshold)
        similarOD2 = metric.similar(coords0[:2], coords1[:2], similarity_threshold)
        similarOD3 = metric.similar(coords0[2:], coords1[2:], similarity_threshold)

        ## Sub-Test 4 :
        # random, but, only origin points are sampled within a circle and should always be < sim threshold
        # Since origin of two points lies within threshold limits,they should be similar
        # when we check just origin for similarity.


        trips = etmm.generate_mock_trips('alice', 2, [0, 0], [1, 1], 'o_',threshold=generate_points_thresh)        
        coords0 = metric.extract_features(trips[0])[:2]
        coords1 = metric.extract_features(trips[1])[:2]        
        similarO = metric.similar(coords0, coords1, similarity_threshold)

        ##Sub-Test 5 :
        # random, but, only destination points are sampled within a circle and should always be < sim threshold
        # Since destination of two points lies within threshold limits,they should be similar
        # when we check just destination for similarity.

        trips = etmm.generate_mock_trips('Caty', 2, [0, 0], [1, 1], '_d',threshold=generate_points_thresh)        
        coords0 = metric.extract_features(trips[0])[2:]
        coords1 = metric.extract_features(trips[1])[2:]        
        similarD = metric.similar(coords0, coords1, similarity_threshold)

        # All the similars must be true
        self.assertTrue(similarOD1) # RESULT SUB-TEST 1
        self.assertTrue(similarOD2) # RESULT SUB-TEST 2
        self.assertTrue(similarOD3) # RESULT SUB-TEST 3
        self.assertTrue(similarO)  # RESULT SUB-TEST 4
        self.assertTrue(similarD) # RESULT SUB-TEST 5
    
    def testODsAreNotSimilar(self):
        generate_points_thresh = 0.001  # approx. 111 meters
        similarity_threshold = 111  # 
        metric = eamso.OriginDestinationSimilarity()

        ## Sub-Test 1-2: 
        # Two trips with neither origin nor destination coordinates within threshold
        # must not be similar in any configuration of similarity testing.
        trips = etmm.generate_mock_trips('bob', 2, [0, 0], [1, 1], '__', threshold=generate_points_thresh)  
        coords0 = metric.extract_features(trips[0])
        coords1 = metric.extract_features(trips[1])
        similar11 = metric.similar(coords0[:2], coords1[:2], similarity_threshold)
        similar12 = metric.similar(coords0[2:], coords1[:], similarity_threshold)

        ## Sub-Test 3-4: 
        # Two trips with  origin coordinates within threshold but we check  
        # similarity using destination coordinates or origin-and-destination
        # should not be similar.
        trips = etmm.generate_mock_trips('Alice', 2, [2, 2], [3, 3], 'o_', threshold=generate_points_thresh)
        metric = eamso.OriginDestinationSimilarity()
        coords0 = metric.extract_features(trips[0])
        coords1 = metric.extract_features(trips[1])
        similar21 = metric.similar(coords0[2:], coords1[2:], similarity_threshold)
        similar22 = metric.similar(coords0, coords1, similarity_threshold)

        ## Sub-Test 5-6: 
        # Two trips with destination coordinates within threshold but we check 
        # similarity using origin coordinates or origin-and-destination 
        # should not be similar.        
        trips = etmm.generate_mock_trips('Caty', 2, [3, 3], [4, 4], '_d', threshold=generate_points_thresh)
        metric = eamso.OriginDestinationSimilarity()
        coords0 = metric.extract_features(trips[0])
        coords1 = metric.extract_features(trips[1])
        similar31 = metric.similar(coords0[:2], coords1[:2], similarity_threshold)
        similar32 = metric.similar(coords0, coords1, similarity_threshold)

        # All the similars must be False
        self.assertFalse(similar11) # RESULT SUB-TEST 1
        self.assertFalse(similar12) # RESULT SUB-TEST 2
        self.assertFalse(similar21) # RESULT SUB-TEST 3
        self.assertFalse(similar22) # RESULT SUB-TEST 4
        self.assertFalse(similar31) # RESULT SUB-TEST 5
        self.assertFalse(similar32) # RESULT SUB-TEST 6


if __name__ == '__main__':
    unittest.main()
