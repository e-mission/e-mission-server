from abc import ABCMeta, abstractmethod
from typing import List
import logging

import emission.core.wrapper.confirmedtrip as ecwc


class SimilarityMetric(metaclass=ABCMeta):

    @abstractmethod
    def extract_features(self, trip: ecwc.Confirmedtrip) -> List[float]:
        """extracts the features we want to compare for similarity

        :param trip: a confirmed trip
        :return: the features to compare
        """
        pass

    @abstractmethod
    def similarity(self, a: List[float], b: List[float], clustering_way = 'origin-destination') -> List[float]:
        """compares the features, producing their similarity
        as computed by this similarity metric

        :param a: features for a trip
        :param b: features for another trip
        :param clustering_way : takes one among 'origin', 'destination', 'origin-destination' as value.
                                tells the part of the trip to be used for binning trips together if that 
                                part lies within a threshold.
        :return: for each feature, the similarity of these features
        """
        pass

    def similar(self, a: List[float], b: List[float], thresh: float, clustering_way= 'origin-destination') -> bool:
        """compares the features, returning true if they are similar
        within some threshold

        :param a: features for a trip 
        :param b: features for another trip
        :param thresh: threshold for similarity
        :param clustering_way : takes one among 'origin', 'destination', 'origin-destination' as value.
                                tells the part of the trip to be used for binning trips together if that 
                                part lies within a threshold.
        :return: true if the feature similarity is within some threshold
        """
        similarity_values = self.similarity(a, b, clustering_way)
        is_similar = all(sim <= thresh for sim in similarity_values)

        return is_similar
