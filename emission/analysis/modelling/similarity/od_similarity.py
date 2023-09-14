from typing import List
import emission.analysis.modelling.similarity.similarity_metric as eamss
import emission.analysis.modelling.similarity.confirmed_trip_feature_extraction as ctfe
import emission.core.wrapper.confirmedtrip as ecwc
import emission.core.common as ecc


class OriginDestinationSimilarity(eamss.SimilarityMetric):
    """
    similarity metric which compares, for two trips, 
    the distance for origin to origin, and destination to destination,
    in meters.
    """
    
    def extract_features(self, trip: ecwc.Confirmedtrip) -> List[float]:
        return ctfe.od_features(trip)

    def similarity(self, a: List[float], b: List[float], clustering_way='origin-destination') -> List[float]:
        """
        a : a list of point features that takes the forms
          [point1_longitude,point1_latitude,point2_longitude,point2_latitude] 
                    
        b : a list of point features that takes the forms
          [point1_longitude,point1_latitude,point2_longitude,point2_latitude] 
        
        clustering_way : takes one among 'origin', 'destination', 'origin-destination' as value.
                         tells the part of the trip to be used for binning trips together if that 
                         part lies within threshold.
                                                  
        return: a list of size 1 ([distance between point1-point3]) if a and b take form 1
                or of size 2 ([distance between point1-point3, distance between point2-point4])
                if a and b take form 2.
        """
        origin_dist = ecc.calDistance(a[0:2], b[0:2])
        destination_dist=ecc.calDistance(a[2:4], b[2:4])

        if clustering_way == 'origin-destination':
            return [origin_dist,destination_dist]
        elif clustering_way == 'origin':
            return [origin_dist]
        else:
            return [destination_dist]