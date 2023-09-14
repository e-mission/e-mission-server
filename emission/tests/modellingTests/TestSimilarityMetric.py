import unittest
import emission.analysis.modelling.similarity.od_similarity as eamso
import emission.tests.modellingTests.modellingTestAssets as etmm
class TestSimilarityMetric(unittest.TestCase):

    def testODsAreSimilar(self):
        similarity_threshold = 500  # in meters
        metric = eamso.OriginDestinationSimilarity()

        # parameters passed for testing is set here. A list, where each element of this list takes the form 
        # [trip part to be sampled within mentioned threshold, (start_coord,end_coord)]
        # Since the extracted_features function returns in the form [origin_lat,origin_long,destination_lat,destination_long],
        # if clustering is to be done by :
        #   a.origin, we pass first two values of this list,i.e. from 0 till before 2 index
        #   b.destination, we pas last two values of this list,i.e. from 2 till before 4 index
        #   c.origin-destination, we pass the entire list , i.e. from 0 till before 4 index
        parameters= [["o_",'origin'],["_d",'destination'],["od",'origin-destination']]

        for tp,cw in parameters:
            with self.subTest(trip_part=tp):
                #generate 2 trips with parameter values
                trips = etmm.generate_mock_trips('joe',2, threshold=similarity_threshold,origin=[0, 0], destination=[1, 1], within_threshold=2,trip_part=tp) 
                # depending on the parametrs, extract the relevant coordinates
                trip0_coords = metric.extract_features(trips[0])
                trip1_coords = metric.extract_features(trips[1])
                #check for similarity using relevant coordinates
                similarOD = metric.similar(trip0_coords,trip1_coords, similarity_threshold,cw)
                # Since both origin and destination poitns lie within threshold limits,they should be similar
                # when we check by just origin or just destination or both origin-and-destination
                self.assertTrue(similarOD)
    
    def testODsAreNotSimilar(self):
        similarity_threshold = 500
        metric = eamso.OriginDestinationSimilarity()

        # parameters passed for testing is set. A list, where each element of this list takes the form 
        # [(start_coord,end_coord)]
        # Since the extracted_features function return in the form [origin_lat,origin_long,destination_lat,destination_long],
        # if clustering shouldn't happend, then
        #   a.origin, we pass first two values of this list,i.e. from 0 till before 2 index
        #   b.destination, we pas last two values of this list,i.e. from 2 till before 4 index
        #   c.origin-destination, we pass the entire list , i.e. from 0 till before 4 index
        parameters= ['origin','destination','origin-destination']
        n=2
        #this generates 2 trips one-by-one, where each trip's respective origin and destination 
        # points are more than 500m away.
        trips = [ etmm.generate_mock_trips('joe',2, origin=[i, i], destination=[i+1, i+1], trip_part= 'od', within_threshold=1,threshold=500)[0] for i in range(n)]    
        trip0_coord = metric.extract_features(trips[0])
        trip1_coord = metric.extract_features(trips[1])

        for cw in parameters:
            with self.subTest(clustering_way=cw):      
                IsSimilar = metric.similar(trip0_coord,trip1_coord, similarity_threshold,cw)
                # Two trips with neither origin nor destination coordinates within the threshold
                # must not be similar by any configuration of similarity testing.
                self.assertFalse(IsSimilar)

if __name__ == '__main__':
    unittest.main()
