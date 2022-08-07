from abc import ABCMeta, abstractmethod
from typing import Dict, List, Tuple

import emission.core.wrapper.confirmedtrip as ecwc


class TripModel(metaclass=ABCMeta):

    @abstractmethod
    def fit(data: List[List[float]]):
        """
        train the model on data in an unsupervised learning setting.

        :param data: 2D array of features to train from
        :type data: List[List[float]]
        """
        pass

    @abstractmethod
    def predict(self, data: List[float]) -> Tuple[List[Dict], int]:
        """use this model to predict labels for some data

        :param data: a single row of features in the model's feature space
        :type data: List[float]
        :return: the predictions and the total count of observations
        :rtype: Tuple[List[Prediction], int]
        """
        pass

    @abstractmethod
    def to_dict(self) -> Dict:
        """
        export the model as a python Dict, to be stored via the file
        system or a document database. 
        
        should be serializable. supported types at this time 
        (2022-05-19) include all built-in Python types and Numpy types.
 
        :return: the model as a Dict
        :rtype: Dict
        """
        pass

    @abstractmethod
    def from_dict(self, model: Dict):
        """
        import the model from a python Dict that was stored in the file
        system or a database. forms a codec which should be idempotent
        when composed with to_dict.

        :param model: the model as a python Dict
        :type model: Dict
        """
        pass

    @property
    @abstractmethod
    def is_incremental(self) -> bool:
        """
        whether this model requires the complete user history to build (False),
        or, if only the incremental data since last execution is required (True).

        :return: if the model is incremental. the current timestamp will be recorded
        in the analysis pipeline. the next call to this model will only include 
        trip data for trips later than the recorded timestamp.
        :rtype: bool
        """
        pass

    @abstractmethod
    def extract_features(self, trip: ecwc.Confirmedtrip) -> List[float]:
        """
        extract the relevant features for learning from a trip for this model instance

        :param trip: the trip to extract features from
        :type trip: Confirmedtrip
        :return: a vector containing features to predict from
        :rtype: List[float]
        """
        pass
