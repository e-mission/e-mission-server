from abc import ABCMeta, abstractmethod
from typing import List

from emission.core.wrapper.confirmedtrip import Confirmedtrip


class SimilarityMetric(metaclass=ABCMeta):

    @abstractmethod
    def extract_features(self, trip: Confirmedtrip) -> List[float]:
        """extracts the features we want to compare for similarity

        :param trip: a confirmed trip
        :type trip: Confirmedtrip
        :return: the features to compare
        :rtype: List[float]
        """
        pass

    def similarity(self, a: List[float], b: List[float]) -> List[float]:
        """compares the features, producing their similarity
        as computed by this similarity metric

        :param a: features for a trip
        :type a: List[float]
        :param b: features for another trip
        :type b: List[float]
        :return: for each feature, the similarity of these features
        :rtype: List[float]
        """
        pass

    def similar(self, a: List[float], b: List[float], thresh: float) -> bool:
        """compares the features, returning true if they are similar
        within some threshold

        :param a: features for a trip
        :type a: List[float]
        :param b: features for another trip
        :type b: List[float]
        :param thresh: threshold for similarity
        :type thresh: float
        :return: true if the feature similarity is within some threshold
        :rtype: float
        """
        pass