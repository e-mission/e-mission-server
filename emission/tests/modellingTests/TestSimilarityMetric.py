import unittest
import emission.analysis.modelling.similarity.od_similarity as eamso
import emission.tests.modellingTests.utilities as etmu

class TestSimilarityMetric(unittest.TestCase):

    def testODsAreSimilar(self):
        generate_points_thresh = 0.001  # approx. 111 meters
        similarity_threshold = 500  # in meters
        metric = eamso.OriginDestinationSimilarity()

        # parameters passed for testing is set here. A list, where each element of this list takes the form 
        # [trip part to be sampled within mentioned threshold, (start_coord,end_coord)]
        # Since the extracted_features function returns in the form [origin_lat,origin_long,destination_lat,destination_long],
        # if clustering is to be done by :
        #   a.origin, we pass first two values of this list,i.e. from 0 till before 2 index
        #   b.destination, we pas last two values of this list,i.e. from 2 till before 4 index
        #   c.origin-destination, we pass the entire list , i.e. from 0 till before 4 index
        parameters= [["od",(0,4)],["_d",(2,4)],["o_",(0,2)]]

        for tp,(coord_start,coord_end) in parameters:
            with self.subTest(trip_part=tp):
                #generate 2 trips with parameter values
                trips = etmu.setTripConfig(2, [0, 0], [1, 1], trip_part=tp,threshold=generate_points_thresh) 
                # depending on the parametrs, extract the relevant coordinates
                trip0_coords = metric.extract_features(trips[0])[coord_start:coord_end]
                trip1_coords = metric.extract_features(trips[1])[coord_start:coord_end]
                #check for similarity using relevant coordinates
                similarOD = metric.similar(trip0_coords,trip1_coords, similarity_threshold)
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
        parameters= [(0,2),(2,4),[0,4]]
        n=2
        #this generates 2 trips one-by-one, where each trip's respective origin and destination 
        # points are more than 500m away.
        trips = [etmu.setTripConfig(1, (i, i), (i+1, i+1), 'od', 1)[0] for i in range(n)]    
        trip0_coord = metric.extract_features(trips[0])
        trip1_coord = metric.extract_features(trips[1])

        for (coord_start,coord_end) in parameters:
            with self.subTest(coordinates=(coord_start,coord_end)):      
                IsSimilar = metric.similar(trip0_coord[coord_start:coord_end],trip1_coord[coord_start:coord_end], similarity_threshold)
                # Two trips with neither origin nor destination coordinates within the threshold
                # must not be similar by any configuration of similarity testing.
                self.assertFalse(IsSimilar)

if __name__ == '__main__':
    unittest.main()
