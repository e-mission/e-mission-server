import unittest
import emission.tests.modellingTests.modellingTestAssets as etmm
import emission.analysis.modelling.similarity.od_similarity as eamso

class TestSimilarityMetric(unittest.TestCase):

    def testODsAreSimilar(self):
        generate_points_thresh = 0.001  # approx. 111 meters
        similarity_threshold = 500  # 
        # random, but, points are sampled within a circle and should always be < sim threshold
        trips = etmm.generate_mock_trips('bob', 2, [0, 0], [1, 1], threshold=generate_points_thresh)
        metric = eamso.OriginDestinationSimilarity()
        coords0 = metric.extract_features(trips[0])
        coords1 = metric.extract_features(trips[1])
        similar = metric.similar(coords0, coords1, similarity_threshold)
        self.assertTrue(similar)
    
    def testODsAreNotSimilar(self):
        generate_points_thresh = 0.001  # approx. 111 meters
        similarity_threshold = 500  # 
        
        trips0 = etmm.generate_mock_trips('bob', 1, [0, 0], [1, 1], threshold=generate_points_thresh)
        trips1 = etmm.generate_mock_trips('alice', 1, [2, 2], [3, 3], threshold=generate_points_thresh)
        metric = eamso.OriginDestinationSimilarity()
        coords0 = metric.extract_features(trips0[0])
        coords1 = metric.extract_features(trips1[0])
        similar = metric.similar(coords0, coords1, similarity_threshold)
        self.assertFalse(similar)

if __name__ == '__main__':
    unittest.main()
