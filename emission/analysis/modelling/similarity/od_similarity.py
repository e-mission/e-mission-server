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

    def similarity(self, a: List[float], b: List[float], thresh: float) -> List[float]:
        o_dist = ecc.calDistance([a[0], a[1]], [b[0], b[1]])
        d_dist = ecc.calDistance([a[2], a[3]], [b[2], b[3]])
        return [o_dist, d_dist]