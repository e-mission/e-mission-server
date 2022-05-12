from typing import List
from emission.analysis.modelling.similarity.similarity_metric import SimilarityMetric
import emission.analysis.modelling.similarity.confirmed_trip_feature_extraction as ctfe
from emission.analysis.modelling.tour_model.similarity import similarity
from emission.core.wrapper.confirmedtrip import Confirmedtrip
import emission.core.common as ecc


class OriginDestinationSimilarity(SimilarityMetric):
    """
    similarity metric which compares, for two trips, 
    the distance for origin to origin, and destination to destination
    """
    
    def extract_features(self, trip: Confirmedtrip) -> List[float]:
        return ctfe.od_features(trip)

    def similarity(self, a: List[float], b: List[float], thresh: float) -> List[float]:
        o_dist = ecc.calDistance([a[0], a[1]], [b[0], b[1]])
        d_dist = ecc.calDistance([a[2], a[3]], [b[2], b[3]])
        return [o_dist, d_dist]

    def similar(self, a: List[float], b: List[float], thresh: float) -> bool:
        o_dist, d_dist = self.similarity(a, b)
        is_similar = o_dist <= thresh and d_dist <= thresh
        return is_similar