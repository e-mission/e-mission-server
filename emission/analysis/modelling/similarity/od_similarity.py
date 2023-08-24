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

    def similarity(self, a: List[float], b: List[float]) -> List[float]:
        """
        a : a list of point features that can take either of two forms
                    1. [point1_latitude,point1_longitude]  
                    2. [point1_latitude,point1_longitude,point2_latitude,point2_longitude] 
                    
        b : a list of point features that can take either of two forms
                    1. [point3_latitude,point3_longitude]  
                    2. [point3_latitude,point3_longitude,point4_latitude,point4_longitude] 
            
            It'll always take the same form as parameter a.

        return: a list of size 1 ([distance between point1-point3]) if a and b take form 1
                or of size 2 ([distance between point1-point3, distance between point2-point4])
                if a and b take form 2.
        """
        
        point_dist = [ecc.calDistance(a[i:i+2], b[i:i+2]) 
                      for i in range (0,len(a),2)] 
        
        return point_dist