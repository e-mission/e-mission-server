from abc import ABCMeta, abstractmethod
from typing import List

from emission.core.wrapper.confirmedtrip import Confirmedtrip


class SimilarityMetric(metaclass=ABCMeta):

    @abstractmethod
    def extract_features(self, trip: Confirmedtrip) -> List[float]:
        """extracts the features we want to compare for similarity

        :param trip: a confirmed trip
        :return: the features to compare
        """
        pass

    @abstractmethod
    def similarity(self, a: List[float], b: List[float]) -> List[float]:
        """compares the features, producing their similarity
        as computed by this similarity metric

        :param a: features for a trip
        :param b: features for another trip
        :return: for each feature, the similarity of these features
        """
        pass

    def similar(self, a: List[float], b: List[float], thresh: float) -> bool:
        """compares the features, returning true if they are similar
        within some threshold

        :param a: features for a trip
        :param b: features for another trip
        :param thresh: threshold for similarity
        :return: true if the feature similarity is within some threshold
        """
        similarity_values = self.similarity(a, b)
        is_similar = all(map(lambda sim: sim <= thresh, similarity_values))
        return is_similar
